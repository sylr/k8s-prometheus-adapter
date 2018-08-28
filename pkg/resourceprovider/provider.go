/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resourceprovider

import (
	"time"
	"context"
	"sync"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metrics "k8s.io/metrics/pkg/apis/metrics"
	"github.com/kubernetes-incubator/metrics-server/pkg/provider"
	"github.com/golang/glog"

	"github.com/directxman12/k8s-prometheus-adapter/pkg/config"
	"github.com/directxman12/k8s-prometheus-adapter/pkg/client"
	"github.com/directxman12/k8s-prometheus-adapter/pkg/naming"
	pmodel "github.com/prometheus/common/model"
)

var (
	nodeResource = schema.GroupResource{Resource: "nodes"}
	nsResource = schema.GroupResource{Resource: "ns"}
	podResource = schema.GroupResource{Resource: "pods"}
)

// TODO(directxman12): consider support for nanocore values -- adjust scale if less than 1 millicore, or greater than max int64

func NewProvider(prom client.Client, mapper apimeta.RESTMapper, cfg *config.ResourceRules) (provider.MetricsProvider, error) {
	cpuConverter, err := naming.NewResourceConverter(cfg.CPU.Resources.Template, cfg.CPU.Resources.Overrides, mapper)
	if err != nil {
		return nil, fmt.Errorf("unable to construct label-resource converter for CPU: %v", err)
	}
	memConverter, err := naming.NewResourceConverter(cfg.Memory.Resources.Template, cfg.Memory.Resources.Overrides, mapper)
	if err != nil {
		return nil, fmt.Errorf("unable to construct label-resource converter for memory: %v", err)
	}

	cpuContQuery, err := naming.NewMetricsQuery(cfg.CPU.ContainerQuery, cpuConverter)
	if err != nil {
		return nil, fmt.Errorf("unable to construct container metrics query for CPU: %v", err)
	}
	memContQuery, err := naming.NewMetricsQuery(cfg.Memory.ContainerQuery, memConverter)
	if err != nil {
		return nil, fmt.Errorf("unable to construct container metrics query for memory: %v", err)
	}
	cpuNodeQuery, err := naming.NewMetricsQuery(cfg.CPU.NodeQuery, cpuConverter)
	if err != nil {
		return nil, fmt.Errorf("unable to construct node metrics query for CPU: %v", err)
	}
	memNodeQuery, err := naming.NewMetricsQuery(cfg.Memory.NodeQuery, memConverter)
	if err != nil {
		return nil, fmt.Errorf("unable to construct node metrics query for memory: %v", err)
	}

	return &resourceProvider{
		prom: prom,
		cpu: resQuery{
			converter: cpuConverter,
			contQuery: cpuContQuery,
			nodeQuery: cpuNodeQuery,
		},
		mem: resQuery{
			converter: memConverter,
			contQuery: memContQuery,
			nodeQuery: memNodeQuery,
		},
		window: time.Duration(cfg.Window),
	}, nil
}

type resQuery struct {
	converter naming.ResourceConverter
	contQuery naming.MetricsQuery
	nodeQuery naming.MetricsQuery
}

type resourceProvider struct {
	prom client.Client

	cpu, mem resQuery

	window time.Duration
}

type nsQueryResults struct {
	namespace string
	cpu, mem queryResults
	err error
}

func (p *resourceProvider) GetContainerMetrics(pods ...apitypes.NamespacedName) ([]provider.TimeInfo, [][]metrics.ContainerMetrics, error) {
	if len(pods) == 0 {
		return nil, nil, fmt.Errorf("no pods to fetch metrics for")
	}

	// TODO(directxman12): figure out how well this scales if we go to list 1000+ pods
	// (and consider adding timeouts)

	// group pods by namespace (we could be listing for all pods in the cluster)
	podsByNs := make(map[string][]string, len(pods))
	for _, pod := range pods {
		podsByNs[pod.Namespace] = append(podsByNs[pod.Namespace], pod.Name)
	}

	now := pmodel.Now()
	resChan := make(chan nsQueryResults, len(podsByNs))
	var wg sync.WaitGroup
	wg.Add(len(podsByNs))

	for ns, podNames := range podsByNs {
		go func(ns string, podNames []string) {
			defer wg.Done()
			resChan <- p.queryBoth(now, podResource, true, ns, podNames...)
		}(ns, podNames)
	}

	wg.Wait()
	close(resChan)

	resultsByNs := make(map[string]nsQueryResults, len(podsByNs))
	for result := range resChan {
		if result.err != nil {
			glog.Errorf("unable to fetch metrics for pods in namespace %q, skipping: %v", result.namespace, result.err)
			continue
		}
		resultsByNs[result.namespace] = result
	}

	resTimes := make([]provider.TimeInfo, len(pods))
	resMetrics := make([][]metrics.ContainerMetrics, len(pods))

	for i, pod := range pods {
		nsRes, nsResPresent := resultsByNs[pod.Namespace]
		if !nsResPresent {
			glog.Errorf("unable to fetch metrics for pods in namespace %q, skipping pod %s", pod.Namespace, pod.String())
			continue
		}
		cpuRes, hasResult := nsRes.cpu[pod.Name]
		if !hasResult {
			glog.Errorf("unable to fetch CPU metrics for pod %s, skipping", pod.String())
			continue
		}
		memRes, hasResult := nsRes.mem[pod.Name]
		if !hasResult {
			glog.Errorf("unable to fetch memory metrics for pod %s, skipping", pod.String())
			continue
		}

		earliestTs := pmodel.Latest

		containerMetrics := make(map[string]metrics.ContainerMetrics)
		for _, cpu := range cpuRes {
			containerName := string(cpu.Metric[pmodel.LabelName(p.cpu.containerLabel)])
			if _, present := containerMetrics[containerName]; !present {
				containerMetrics[containerName] = metrics.ContainerMetrics{
					Name: containerName,
					Usage: corev1.ResourceList{},
				}
			}
			containerMetrics[containerName].Usage[corev1.ResourceCPU] = *resource.NewMilliQuantity(int64(cpu.Value*1000.0), resource.DecimalSI)
			if cpu.Timestamp.Before(earliestTs) {
				earliestTs = cpu.Timestamp
			}
		}
		for _, mem := range memRes {
			containerName := string(mem.Metric[pmodel.LabelName(p.mem.containerLabel)])
			if _, present := containerMetrics[containerName]; !present {
				containerMetrics[containerName] = metrics.ContainerMetrics{
					Name: containerName,
					Usage: corev1.ResourceList{},
				}
			}
			containerMetrics[containerName].Usage[corev1.ResourceMemory] = *resource.NewMilliQuantity(int64(mem.Value*1000.0), resource.BinarySI)
			if mem.Timestamp.Before(earliestTs) {
				earliestTs = mem.Timestamp
			}
		}

		resTimes[i] = provider.TimeInfo{
			Timestamp: earliestTs.Time(),
			Window: p.window,
		}

		for _, containerMetric := range containerMetrics {
			resMetrics[i] = append(resMetrics[i], containerMetric)
		}
	}

	return resTimes, resMetrics, nil
}

func (p *resourceProvider) GetNodeMetrics(nodes ...string) ([]provider.TimeInfo, []corev1.ResourceList, error) {
	if len(nodes) == 0 {
		return nil, nil, fmt.Errorf("no nodes to fetch metrics for")
	}

	now := pmodel.Now()

	qRes := p.queryBoth(now, nodeResource, false, "", nodes...)
	if qRes.err != nil {
		return nil, nil, qRes.err
	}

	resTimes := make([]provider.TimeInfo, len(nodes))
	resMetrics := make([]corev1.ResourceList, len(nodes))

	for i, nodeName := range nodes {
		rawCPUs, gotResult := qRes.cpu[nodeName]
		if !gotResult {
			glog.V(1).Infof("missing CPU for node %q, skipping", nodeName)
			continue
		}
		rawMems, gotResult := qRes.mem[nodeName]
		if !gotResult {
			glog.V(1).Infof("missing memory for node %q, skipping", nodeName)
			continue
		}

		rawMem := rawMems[0]
		rawCPU := rawCPUs[0]

		resMetrics[i] = corev1.ResourceList{
			corev1.ResourceCPU: *resource.NewMilliQuantity(int64(rawCPU.Value*1000.0), resource.DecimalSI),
			corev1.ResourceMemory: *resource.NewMilliQuantity(int64(rawMem.Value*1000.0), resource.BinarySI),
		}

		if rawMem.Timestamp.Before(rawCPU.Timestamp) {
			resTimes[i] = provider.TimeInfo{
				Timestamp: rawMem.Timestamp.Time(),
				Window: p.window,
			}
		} else {
			resTimes[i] = provider.TimeInfo{
				Timestamp: rawCPU.Timestamp.Time(),
				Window: 1*time.Minute,
			}
		}
	}

	return resTimes, resMetrics, nil
}

func (p *resourceProvider) queryBoth(now pmodel.Time, resource schema.GroupResource, isContainer bool, namespace string, names ...string) nsQueryResults {
	var cpuRes, memRes queryResults
	var cpuErr, memErr error

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		cpuRes, cpuErr = p.runQuery(now, p.cpu, resource, isContainer, "", names...)
	}()
	go func() {
		defer wg.Done()
		memRes, memErr = p.runQuery(now, p.mem, resource, isContainer, "", names...)
	}()
	wg.Wait()

	if cpuErr != nil {
		return nsQueryResults{
			namespace: namespace,
			err: fmt.Errorf("unable to fetch node CPU metrics: %v", cpuErr),
		}
	}
	if memErr != nil {
		return nsQueryResults{
			namespace: namespace,
			err: fmt.Errorf("unable to fetch node memory metrics: %v", memErr),
		}
	}

	return nsQueryResults{
		namespace: namespace,
		cpu: cpuRes,
		mem: memRes,
	}
}

// queryResults maps an object name to all the results matching that object
type queryResults map[string][]*pmodel.Sample

func (p *resourceProvider) runQuery(now pmodel.Time, queryInfo resQuery, resource schema.GroupResource, isContainer bool, namespace string, names ...string) (queryResults, error) {
	var extraGroupBy []string
	if isContainer {
		extraGroupBy = []string{queryInfo.containerLabel}
	}
	var query client.Selector
	var err error
	if resource == nodeResource {
		query, err = queryInfo.nodeQuery.Build("", resource, namespace, extraGroupBy, names...)
	} else {
		query, err = queryInfo.contQuery.Build("", resource, namespace, extraGroupBy, names...)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to construct query: %v", err)
	}

	rawRes, err := p.prom.Query(context.Background(), now, query)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query: %v", err)
	}

	resourceLbl, err := queryInfo.converter.LabelForResource(resource)
	if err != nil {
		return nil, fmt.Errorf("unable to find label for resource %s: %v", resource.String(), err)
	}

	if rawRes.Type != pmodel.ValVector || rawRes.Vector == nil {
		return nil, fmt.Errorf("invalid or empty value of non-vector type (%s) returned", rawRes.Type)
	}

	res := make(queryResults, len(*rawRes.Vector))
	for _, val := range *rawRes.Vector {
		if val == nil {
			// skip empty values
			continue
		}
		resKey := string(val.Metric[resourceLbl])
		res[resKey] = append(res[resKey], val)
	}

	return res, nil
}
