package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/zalando-incubator/es-operator/operator/null"
	"gopkg.in/resty.v1"
	v1 "k8s.io/api/core/v1"
)

// TODO make configurable as flags.
var (
	defaultRetryCount       = 999
	defaultRetryWaitTime    = 10 * time.Second
	defaultRetryMaxWaitTime = 30 * time.Second
)

// ESClient is a pod drainer which can drain data from Elasticsearch pods.
type ESClient struct {
	Endpoint             *url.URL
	mux                  sync.Mutex
	excludeSystemIndices bool
}

// ESIndex represent an index to be used in public APIs
type ESIndex struct {
	Index     string `json:"index"`
	Primaries int32  `json:"pri"`
	Replicas  int32  `json:"rep"`
}

// ESShard represent a single shard from the response of _cat/shards
type ESShard struct {
	IP    string `json:"ip"`
	Index string `json:"index"`
}

// ESNode represent a single Elasticsearch node to be used in public API
type ESNode struct {
	IP              string  `json:"ip"`
	DiskUsedPercent float64 `json:"dup"`
}

// _ESIndex represent an index from the response of _cat/indices (only used internally!)
type _ESIndex struct {
	Index     string `json:"index"`
	Primaries string `json:"pri"`
	Replicas  string `json:"rep"`
}

// _ESNode represent a single Elasticsearch node from the response of _cat/nodes (only used internally)
type _ESNode struct {
	IP              string `json:"ip"`
	DiskUsedPercent string `json:"dup"`
}

type ESHealth struct {
	Status string `json:"status"`
}

type Exclude struct {
	IP null.String `json:"_ip,omitempty"`
}

type Allocation struct {
	Exclude Exclude `json:"exclude,omitempty"`
}
type Rebalance struct {
	Enable null.String `json:"enable,omitempty"`
}

type Routing struct {
	Allocation Allocation `json:"allocation,omitempty"`
	Rebalance  Rebalance  `json:"rebalance,omitempty"`
}

type Cluster struct {
	Routing Routing `json:"routing,omitempty"`
}

type ClusterSettings struct {
	Cluster Cluster `json:"cluster"`
}

// ESSettings represent response from _cluster/settings
type ESSettings struct {
	Transient  ClusterSettings `json:"transient,omitempty"`
	Persistent ClusterSettings `json:"persistent,omitempty"`
}

func deduplicateIPs(excludedIPsString string) string {
	if excludedIPsString == "" {
		return ""
	}

	uniqueIPsMap := make(map[string]struct{})
	uniqueIPsList := []string{}
	excludedIPs := strings.Split(excludedIPsString, ",")
	for _, excludedIP := range excludedIPs {
		if _, ok := uniqueIPsMap[excludedIP]; !ok {
			uniqueIPsMap[excludedIP] = struct{}{}
			uniqueIPsList = append(uniqueIPsList, excludedIP)
		}
	}

	return strings.Join(uniqueIPsList, ",")
}

func (esSettings *ESSettings) MergeNonEmptyTransientSettings() {
	if value := esSettings.GetTransientRebalance().ValueOrZero(); value != "" {
		esSettings.Persistent.Cluster.Routing.Rebalance.Enable = null.StringFromPtr(&value)
		esSettings.Transient.Cluster.Routing.Rebalance.Enable = null.StringFromPtr(nil)
	}

	transientExcludeIps := esSettings.GetTransientExcludeIPs().ValueOrZero()
	persistentExcludeIps := esSettings.GetPersistentExcludeIPs().ValueOrZero()
	if persistentExcludeIps == "" && transientExcludeIps != "" {
		esSettings.Persistent.Cluster.Routing.Allocation.Exclude.IP = null.StringFromPtr(&transientExcludeIps)
	} else if persistentExcludeIps != "" {
		uniqueIps := deduplicateIPs(mergeExcludeIpStrings(transientExcludeIps, persistentExcludeIps))
		mergedIps := null.StringFrom(uniqueIps)
		esSettings.Persistent.Cluster.Routing.Allocation.Exclude.IP = mergedIps
	}
	esSettings.Transient.Cluster.Routing.Allocation.Exclude.IP = null.StringFromPtr(nil)
}

func mergeExcludeIpStrings(transientExcludeIps string, persistentExcludeIps string) string {
	if transientExcludeIps == "" {
		return persistentExcludeIps
	}
	return transientExcludeIps + "," + persistentExcludeIps
}

func (esSettings *ESSettings) GetTransientExcludeIPs() null.String {
	return esSettings.Transient.Cluster.Routing.Allocation.Exclude.IP
}

func (esSettings *ESSettings) GetPersistentExcludeIPs() null.String {
	return esSettings.Persistent.Cluster.Routing.Allocation.Exclude.IP
}

func (esSettings *ESSettings) GetTransientRebalance() null.String {
	return esSettings.Transient.Cluster.Routing.Rebalance.Enable
}

func (esSettings *ESSettings) GetPersistentRebalance() null.String {
	return esSettings.Persistent.Cluster.Routing.Rebalance.Enable
}

func (c *ESClient) logger() *log.Entry {
	return log.WithFields(log.Fields{
		"endpoint": c.Endpoint,
	})
}

// Drain drains data from an Elasticsearch pod.
func (c *ESClient) Drain(ctx context.Context, pod *v1.Pod) error {

	c.logger().Info("Ensuring cluster is in green state")

	err := c.ensureGreenClusterState()
	if err != nil {
		return err
	}
	c.logger().Info("Disabling auto-rebalance")
	esSettings, err := c.getClusterSettings()
	if err != nil {
		return err
	}

	err = c.updateAutoRebalance("none", esSettings)
	if err != nil {
		return err
	}
	c.logger().Infof("Excluding pod %s/%s from shard allocation", pod.Namespace, pod.Name)
	err = c.excludePodIP(pod)
	if err != nil {
		return err
	}

	c.logger().Info("Waiting for draining to finish")
	return c.waitForEmptyEsNode(ctx, pod)
}

func (c *ESClient) Cleanup(ctx context.Context) error {

	// 1. fetch IPs from _cat/nodes
	nodes, err := c.GetNodes()
	if err != nil {
		return err
	}

	// 2. fetch exclude._ip settings
	esSettings, err := c.getClusterSettings()
	if err != nil {
		return err
	}

	// 3. clean up exclude._ip settings based on known IPs from (1)
	excludedIPsString := esSettings.GetPersistentExcludeIPs().ValueOrZero()
	excludedIPs := strings.Split(excludedIPsString, ",")
	var newExcludedIPs []string
	for _, excludeIP := range excludedIPs {
		for _, nodeIP := range nodes {
			if excludeIP == nodeIP.IP {
				newExcludedIPs = append(newExcludedIPs, excludeIP)
				sort.Strings(newExcludedIPs)
				break
			}
		}
	}

	newExcludedIPsString := strings.Join(newExcludedIPs, ",")
	if newExcludedIPsString != excludedIPsString {
		c.logger().Infof("Setting exclude list to '%s'", strings.Join(newExcludedIPs, ","))

		// 4. update exclude._ip setting
		err = c.setExcludeIPs(newExcludedIPsString, esSettings)
		if err != nil {
			return err
		}
	}

	if esSettings.GetPersistentRebalance().ValueOrZero() != "all" {
		c.logger().Info("Enabling auto-rebalance")
		return c.updateAutoRebalance("all", esSettings)
	}
	return nil
}

// ensures cluster is in green state
func (c *ESClient) ensureGreenClusterState() error {
	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/_cluster/health?wait_for_status=green&timeout=60s")
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}
	var esHealth ESHealth
	err = json.Unmarshal(resp.Body(), &esHealth)
	if err != nil {
		return err
	}
	if esHealth.Status != "green" {
		return fmt.Errorf("expected 'green', got '%s'", esHealth.Status)
	}
	return nil
}

// returns the response of the call to _cluster/settings
func (c *ESClient) getClusterSettings() (*ESSettings, error) {
	// get _cluster/settings for current exclude list
	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/_cluster/settings")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}
	var esSettings ESSettings
	err = json.Unmarshal(resp.Body(), &esSettings)
	if err != nil {
		return nil, err
	}
	esSettings.MergeNonEmptyTransientSettings()
	return &esSettings, nil
}

// adds the podIP to Elasticsearch exclude._ip list
func (c *ESClient) excludePodIP(pod *v1.Pod) error {

	c.mux.Lock()

	podIP := pod.Status.PodIP

	esSettings, err := c.getClusterSettings()
	if err != nil {
		c.mux.Unlock()
		return err
	}

	excludeString := esSettings.GetPersistentExcludeIPs().ValueOrZero()

	// add pod IP to exclude list
	ips := []string{}
	if excludeString != "" {
		ips = strings.Split(excludeString, ",")
	}
	var foundPodIP bool
	for _, ip := range ips {
		if ip == podIP {
			foundPodIP = true
			break
		}
	}
	if !foundPodIP {
		ips = append(ips, podIP)
		sort.Strings(ips)
		err = c.setExcludeIPs(strings.Join(ips, ","), esSettings)
	}

	c.mux.Unlock()
	return err
}

func (c *ESClient) setExcludeIPs(ips string, originalESSettings *ESSettings) error {
	originalESSettings.updateExcludeIps(ips)
	resp, err := resty.New().R().
		SetHeader("Content-Type", "application/json").
		SetBody(originalESSettings).
		Put(c.Endpoint.String() + "/_cluster/settings")
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}
	return nil
}

func (c *ESClient) forceRollover() error {
	//TODO: alias name should be dynamic
	resp, err := resty.New().R().
		SetHeader("Content-Type", "application/json").
		SetBody(`{
					"conditions": {
					  "max_docs": 1
					}
				  }`).
		Post(c.Endpoint.String() + "/logstash_write/_rollover")
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}

	return nil
}

// adjusts the number of shards according to the number of nodes
func (c *ESClient) updateComponentTemplate() error {
	nodes, err := c.GetNodes()
	if err != nil {
		return err
	}

	// even number of nodes? use half as the number of shards, because we have replicas
	// TODO: make this work with arbitrary number of replicas
	numberOfNodes := len(nodes)
	numberOfShards := numberOfNodes
	if numberOfNodes%2 == 0 {
		numberOfShards = numberOfNodes / 2
	}

	//TODO: pick up the template name from somewhere. Or hardcode it?
	resp, err := resty.New().R().
		SetHeader("Content-Type", "application/json").
		SetBody([]byte(
			fmt.Sprintf(
				`{
					"template": {
					  "settings": {
						"number_of_shards": %d,
						"number_of_replicas": 1
					  }
					}
				  }`,
				numberOfShards,
			),
		)).
		Put(c.Endpoint.String() + "/_component_template/scaling")
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}

	return nil
}

func (c *ESClient) addComponentTemplates(filename string) error {
	//TODO: we'll want to use the value as the filename? So we can put Logstash and generic templates
	//oh, and template name

	templates := make(map[string]string)

	templates["scaling"] = `{
		"template": {
		  "settings": {
			"number_of_shards": 1,
			"number_of_replicas": 1
		  }
		}
	  }`

	templates["logstash"] = `{
		"template": {
		  "settings": {
			"index": {
			  "refresh_interval": "5s"
			}
		  },
		  "mappings": {
			"dynamic_templates": [
			  {
				"message_field": {
				  "path_match": "message",
				  "mapping": {
					"norms": false,
					"type": "text"
				  },
				  "match_mapping_type": "string"
				}
			  },
			  {
				"string_fields": {
				  "mapping": {
					"norms": false,
					"type": "text",
					"fields": {
					  "keyword": {
						"ignore_above": 256,
						"type": "keyword"
					  }
					}
				  },
				  "match_mapping_type": "string",
				  "match": "*"
				}
			  }
			],
			"properties": {
			  "@timestamp": {
				"type": "date"
			  },
			  "geoip": {
				"dynamic": true,
				"properties": {
				  "ip": {
					"type": "ip"
				  },
				  "latitude": {
					"type": "half_float"
				  },
				  "location": {
					"type": "geo_point"
				  },
				  "longitude": {
					"type": "half_float"
				  }
				}
			  },
			  "@version": {
				"type": "keyword"
			  }
			}
		  }
		}
	  }`

	for templateName, templateValue := range templates {
		resp, err := resty.New().R().
			SetHeader("Content-Type", "application/json").
			SetBody([]byte(templateValue)).
			Put(c.Endpoint.String() + "/_component_template/" + templateName)
		if err != nil {
			return err
		}
		if resp.StatusCode() != http.StatusOK {
			return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
		}
	}

	return nil
}

func (c *ESClient) addTemplate(value string) error {
	resp, err := resty.New().R().
		SetHeader("Content-Type", "application/json").
		SetBody(`{
					"index_patterns": ["logstash-*"],
					"composed_of": ["logstash", "scaling"],
					"template": {
					  "settings": {
						"plugins.index_state_management.rollover_alias": "logstash_write"
					  }
					}
				  }`).
		Put(c.Endpoint.String() + "/_index_template/logstash") //TODO of course not necessarily "logstash". Read from file/resource
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}

	return nil
}

func (c *ESClient) getISMPolicyVersionInfo() (int64, int64, error) {
	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "_plugins/_ism/policies/rollover_delete_policy") //TODO: can't we use a client here?
	if resp.StatusCode() == http.StatusNotFound {
		return -1, -1, nil // no policy there
	}
	if err != nil {
		return -1, -1, fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body()) // there was an error with the request
	}

	esResponse := make(map[string]interface{})
	err = json.Unmarshal(resp.Body(), &esResponse)
	if err != nil {
		return -1, -1, err
	}

	var seqNo int64
	var primaryTerm int64

	seqNo = esResponse["_seq_no"].(int64)
	primaryTerm = esResponse["_primary_term"].(int64)

	return seqNo, primaryTerm, nil
}

// add/update a rollover policy
func (c *ESClient) updateISMPolicy() error {
	//we need the number of shards to compute min_size
	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/_component_template/scaling") //TODO: can't we use a client here?
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}
	esResponse := make(map[string]interface{})
	err = json.Unmarshal(resp.Body(), &esResponse)
	if err != nil {
		return err
	}

	var numberOfShards int

	if esResponse["component_template"] != nil {
		templates := esResponse["component_template"].([]map[string]interface{})

		my_template := templates[0]["template"].(map[string]interface{})

		settings := my_template["settings"].(map[string]interface{})

		numberOfShards, err = strconv.Atoi(settings["number_of_shards"].(string))
		if err != nil {
			return fmt.Errorf("can't parse number_of_shards from %s", resp.Body())
		}
	} else {
		numberOfShards = 1 //no template is there yet, assume 1
	}

	numberOfGb := numberOfShards * 10 //TODO configurable?
	minSize := fmt.Sprintf("%dgb", numberOfGb)

	//check if the policy is already there. If it is, we need the _seq_no and _primary_term
	var seqNo int64
	var primaryTerm int64
	seqNo, primaryTerm, err = c.getISMPolicyVersionInfo()

	if err != nil {
		return nil
	}

	versionInfo := ""

	if seqNo != -1 && primaryTerm != -1 {
		versionInfo = fmt.Sprintf("?if_seq_no=%d&if_primary_term=%d", seqNo, primaryTerm)
	}

	//TODO read "meat" of the policy from a file

	resp, err = resty.New().R().
		SetHeader("Content-Type", "application/json").
		SetBody([]byte(
			fmt.Sprintf(
				`{
					"policy": {
					  "description": "Managed rollover policy (min_size will be adjusted with number of shards)",
					  "default_state": "ingest",
					  "states": [
						{
						  "name": "ingest",
						  "actions": [
							{
							  "rollover": {
								"min_size": "%s"
							  }
							}
						  ],
						  "transitions": [
							{
							  "state_name": "search"
							}
						  ]
						},
						{
						  "name": "search",
						  "actions": [],
						  "transitions": [
							{
							  "state_name": "delete",
							  "conditions": {
								"min_index_age": "2m"
							  }
							}
						  ]
						},
						{
						  "name": "delete",
						  "actions": [
							{
							  "delete": {}
							}
						  ],
						  "transitions": []
						}
					  ],
					  "ism_template": {
						"index_patterns": [
						  "logstash-*"
						]
					  }
					}
				  }`,
				minSize,
			),
		)).
		Put(c.Endpoint.String() + fmt.Sprintf("/_plugins/_ism/policies/rollover_delete_policy%s", versionInfo))
		//TODO custom name instead of rollover_delete_policy
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}

	return nil
}

func (c *ESClient) createFirstIndexIfMissing(value string) error {
	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/_aliases")
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}

	var aliases map[string]interface{}
	err = json.Unmarshal(resp.Body(), &aliases)
	if err != nil {
		return err
	}

	var indexFound bool
	for indexName := range aliases {
		if strings.HasPrefix(indexName, "logstash-") { //TODO this would be the index pattern
			indexFound = true
			break
		}
	}

	if !indexFound {
		resp, err := resty.New().R().
			SetHeader("Content-Type", "application/json").
			SetBody(`{
						"aliases": {
						"logstash_write": {
							"is_write_index": true
						}
						}
					}`).
			Put(c.Endpoint.String() + "/logstash-000001") //TODO again, not necessarily Logstash
		if err != nil {
			return err
		}
		if resp.StatusCode() != http.StatusOK {
			return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
		}
	}

	return nil
}

func (esSettings *ESSettings) updateExcludeIps(ips string) {
	esSettings.Persistent.Cluster.Routing.Allocation.Exclude.IP = null.StringFromPtr(&ips)
}

func (c *ESClient) updateAutoRebalance(value string, originalESSettings *ESSettings) error {
	originalESSettings.updateRebalance(value)
	resp, err := resty.New().R().
		SetHeader("Content-Type", "application/json").
		SetBody(originalESSettings).
		Put(c.Endpoint.String() + "/_cluster/settings")
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}

	//TODO this will be somewhere else
	c.addComponentTemplates("dummyvalue")

	c.addTemplate("dummyvalue")

	c.updateISMPolicy()

	c.createFirstIndexIfMissing("dummyvalue")

	return nil
}

func (esSettings *ESSettings) updateRebalance(value string) {
	esSettings.Persistent.Cluster.Routing.Rebalance.Enable = null.StringFromPtr(&value)
}

// repeatedly query shard allocations to ensure success of drain operation.
func (c *ESClient) waitForEmptyEsNode(ctx context.Context, pod *v1.Pod) error {
	// TODO: implement context handling
	podIP := pod.Status.PodIP
	_, err := resty.New().
		SetRetryCount(defaultRetryCount).
		SetRetryWaitTime(defaultRetryWaitTime).
		SetRetryMaxWaitTime(defaultRetryMaxWaitTime).
		AddRetryCondition(
			// It is expected to return (bool, error) pair. Resty will retry
			// in case condition returns true or non nil error.
			func(r *resty.Response) (bool, error) {
				var shards []ESShard
				err := json.Unmarshal(r.Body(), &shards)
				if err != nil {
					return true, err
				}
				// shardIP := make(map[string]bool)
				remainingShards := 0
				for _, shard := range shards {
					if shard.IP == podIP {
						remainingShards++
					}
				}
				c.logger().Infof("Found %d remaining shards on %s/%s (%s)", remainingShards, pod.Namespace, pod.Name, podIP)

				// make sure the IP is still excluded, this could have been updated in the meantime.
				if remainingShards > 0 {
					err = c.excludePodIP(pod)
					if err != nil {
						return true, err
					}
				}
				return remainingShards > 0, nil
			},
		).R().
		Get(c.Endpoint.String() + "/_cat/shards?h=index,ip&format=json")
	if err != nil {
		return err
	}
	return nil
}

func (c *ESClient) GetNodes() ([]ESNode, error) {
	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/_cat/nodes?h=ip,dup&format=json")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}
	var esNodes []_ESNode
	err = json.Unmarshal(resp.Body(), &esNodes)
	if err != nil {
		return nil, err
	}

	// HACK: after-the-fact conversion of string to float from ES response.
	returnStruct := make([]ESNode, 0, len(esNodes))
	for _, node := range esNodes {
		diskUsedPercent, err := strconv.ParseFloat(node.DiskUsedPercent, 64)
		if err != nil {
			if node.DiskUsedPercent != "" {
				c.logger().Warnf("Failed to parse '%s' as float for disk usage on '%s'. Falling back to 0", node.DiskUsedPercent, node.IP)
			}
			diskUsedPercent = 0
		}
		returnStruct = append(returnStruct, ESNode{
			IP:              node.IP,
			DiskUsedPercent: diskUsedPercent,
		})
	}

	return returnStruct, nil
}

func (c *ESClient) GetShards() ([]ESShard, error) {
	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/_cat/shards?h=index,ip&format=json")

	if err != nil {
		return nil, err
	}

	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}
	var esShards []ESShard
	err = json.Unmarshal(resp.Body(), &esShards)
	if err != nil {
		return nil, err
	}
	return esShards, nil
}

func (c *ESClient) GetIndices() ([]ESIndex, error) {
	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/_cat/indices?h=index,pri,rep&format=json")

	if err != nil {
		return nil, err
	}

	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}
	var esIndices []_ESIndex
	err = json.Unmarshal(resp.Body(), &esIndices)
	if err != nil {
		return nil, err
	}

	returnStruct := make([]ESIndex, 0, len(esIndices))
	for _, index := range esIndices {
		// ignore system indices
		if c.excludeSystemIndices && strings.HasPrefix(index.Index, ".") {
			continue
		}
		// HACK: after-the-fact conversion of strings to integers from ES response.
		primaries, err := strconv.Atoi(index.Primaries)
		if err != nil {
			return nil, err
		}
		replicas, err := strconv.Atoi(index.Replicas)
		if err != nil {
			return nil, err
		}
		returnStruct = append(returnStruct, ESIndex{
			Primaries: int32(primaries),
			Replicas:  int32(replicas),
			Index:     index.Index,
		})
	}

	return returnStruct, nil
}

func (c *ESClient) UpdateIndexSettings(indices []ESIndex) error {

	if len(indices) == 0 {
		return nil
	}

	err := c.ensureGreenClusterState()
	if err != nil {
		return err
	}

	for _, index := range indices {
		c.logger().Infof("Setting number_of_replicas for index '%s' to %d.", index.Index, index.Replicas)
		resp, err := resty.New().R().
			SetHeader("Content-Type", "application/json").
			SetBody([]byte(
				fmt.Sprintf(
					`{"index" : {"number_of_replicas" : "%d"}}`,
					index.Replicas,
				),
			)).
			Put(fmt.Sprintf("%s/%s/_settings", c.Endpoint.String(), index.Index))
		if err != nil {
			return err
		}

		if resp.StatusCode() != http.StatusOK {
			// if the index doesn't exist ES would return a 404
			if resp.StatusCode() == http.StatusNotFound {
				log.Warnf("Index '%s' not found, assuming it has been deleted.", index.Index)
				return nil
			}
			return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
		}
	}
	return nil
}

func (c *ESClient) CreateIndex(indexName, groupName string, shards, replicas int) error {
	resp, err := resty.New().R().
		SetHeader("Content-Type", "application/json").
		SetBody([]byte(
			fmt.Sprintf(
				`{"settings": {"index" : {"number_of_replicas" : "%d", "number_of_shards": "%d", 
"routing.allocation.include.group": "%s"}}}`,
				replicas,
				shards,
				groupName,
			),
		)).
		Put(fmt.Sprintf("%s/%s", c.Endpoint.String(), indexName))
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}
	return nil
}

func (c *ESClient) DeleteIndex(indexName string) error {
	resp, err := resty.New().R().
		Delete(fmt.Sprintf("%s/%s", c.Endpoint.String(), indexName))
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}
	return nil
}
