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

// ISMPolicy reporesents the response from /_plugins/_ism/policies/POLICY_NAME_GOES_HERE
type ISMPolicy struct {
	Policy      Policy  `json:"policy"`
	SeqNo       float64 `json:"_seq_no"`
	PrimaryTerm float64 `json:"_primary_term"`
}

// "policy" object from the ISM policy
type Policy struct {
	States []State `json:"states,omitempty"`
}

// each state object from an ISM policy
type State struct {
	Actions []Action `json:"actions,omitempty"`
}

// each action within a state of an ISM policy
type Action struct {
	Rollover *Rollover `json:"rollover,omitempty"`
}

// rollover action properties
type Rollover struct {
	MinSize string `json:"min_size,omitempty"`
}

// essential info from an ISM Policy. Good for passing around
type ISMPolicyInfo struct {
	SeqNo           int64
	PrimaryTerm     int64
	RolloverMinSize string
	err             error
}

// response of a _component_template GET call
type ComponentTemplateResponse struct {
	ComponentTemplates []ComponentTemplate `json:"component_templates,omitempty"`
}

// element of the component_templates array
type ComponentTemplate struct {
	Name                    string                  `json:"name"`
	ComponentTemplateObject ComponentTemplateObject `json:"component_template"`
}

// component_template object
type ComponentTemplateObject struct {
	Template TemplateObject `json:"template"`
}

// template object of a component template
type TemplateObject struct {
	Settings *ComponentTemplateSettings `json:"settings,omitempty"`
}

// settings object of component template
type ComponentTemplateSettings struct {
	Index *ComponentTemplateIndexSettings `json:"index,omitempty"`
}

// index settings object of component template
type ComponentTemplateIndexSettings struct {
	NumberOfShards *string `json:"number_of_shards,omitempty"`
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
		err = c.updateAutoRebalance("all", esSettings)
		if err != nil {
			return err
		}
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
	aliasName := "logstash_write"

	resp, err := resty.New().R().
		SetHeader("Content-Type", "application/json").
		SetBody(`{
					"conditions": {
					  "max_docs": 0
					}
				  }`).
		Post(c.Endpoint.String() + "/" + aliasName + "/_rollover")
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}

	return nil
}

// checks if the shards component template needs updating
func (c *ESClient) getShardsComponentTemplate(templateName string, numberOfShards int) (bool, error) {
	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/_component_template/" + templateName)
	if resp.StatusCode() == http.StatusNotFound {
		return true, nil // component template doesn't exist, we have to create it
	}
	if err != nil {
		// there was an error with the request
		return false, fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}

	var componentTemplateResponse ComponentTemplateResponse
	err = json.Unmarshal(resp.Body(), &componentTemplateResponse)
	if err != nil {
		return false, err
	}

	for _, componentTemplate := range componentTemplateResponse.ComponentTemplates {
		if componentTemplate.Name == templateName {
			settings := componentTemplate.ComponentTemplateObject.Template.Settings
			if settings != nil {
				if settings.Index != nil {
					if settings.Index.NumberOfShards != nil {
						currentNumberOfShards, err := strconv.Atoi(*settings.Index.NumberOfShards)
						if err != nil {
							return false, fmt.Errorf("can't convert %s number_of_shards to int",
								*settings.Index.NumberOfShards)
						}
						if currentNumberOfShards != numberOfShards {
							return true, nil // wrong number of shards, need to update template
						} else {
							return false, nil
						}
					} else {
						return true, nil // no "index" object => needs updating
					}
				} else {
					return true, nil //no "settings" object => needs updating
				}
			}
			break
		}
	}

	// we shouldn't get here unless we can't find the template with our name,
	// though we specifically asked for it. In this case, let's update the template to be sure
	return true, nil
}

// adjusts the number of shards according to the number of nodes. If needed
func (c *ESClient) updateShardsComponentTemplate(numberOfShards int) error {
	templateName := "scaling" //TODO configurable?

	needsUpdating, err := c.getShardsComponentTemplate(templateName, numberOfShards)
	if err != nil {
		return err
	}

	if needsUpdating {
		template := fmt.Sprintf(
			`{
			"template": {
			  "settings": {
				"number_of_shards": %d,
				"number_of_replicas": 1
			  }
			}
		  }`,
			numberOfShards,
		)

		log.Infof("Putting template component: %s", template)

		resp, err := resty.New().R().
			SetHeader("Content-Type", "application/json").
			SetBody(template).
			Put(c.Endpoint.String() + "/_component_template/" + templateName)
		if err != nil {
			return err
		}
		if resp.StatusCode() != http.StatusOK {
			return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
		}
	} else {
		c.logger().Debugf("No need to update %s as it seems to have %d shards", templateName, numberOfShards)
	}

	return nil
}

//add main component template, containing mappings and such, except sharding. If it exists
func (c *ESClient) addMainComponentTemplate() error {
	//TODO: we'll want to use the value as the filename? So we can put Logstash and generic templates
	//oh, and template name..

	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/_component_template/logstash") //TODO again, configurable
	if resp.StatusCode() == http.StatusNotFound {
		templateName := "logstash"

		template := `{
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

		c.logger().Infof(fmt.Sprintf("Putting component template %s with value: %s", templateName, template))
		resp, err := resty.New().R().
			SetHeader("Content-Type", "application/json").
			SetBody([]byte(template)).
			Put(c.Endpoint.String() + "/_component_template/" + templateName)
		if err != nil {
			return err
		}
		if resp.StatusCode() != http.StatusOK {
			return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
		}
	} else {
		if err != nil {
			return err
		}
		c.logger().Debug("Found main component template, not overwriting")
	}

	return nil
}

//upload the index template with our component templates. If it doesn't exist
func (c *ESClient) addTemplate() error {
	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/_index_template/logstash") //TODO again, configurable
	if resp.StatusCode() == http.StatusNotFound {
		templateValue := `{
			"index_patterns": ["logstash-*"],
			"composed_of": ["logstash", "scaling"],
			"template": {
			"settings": {
				"plugins.index_state_management.rollover_alias": "logstash_write"
			}
			}
		}`

		c.logger().Infof("Adding index template with value: %s", templateValue)
		resp, err := resty.New().R().
			SetHeader("Content-Type", "application/json").
			SetBody(templateValue).
			Put(c.Endpoint.String() + "/_index_template/logstash") //TODO of course not necessarily "logstash". Read from file/resource
		if err != nil {
			return err
		}
		if resp.StatusCode() != http.StatusOK {
			return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
		}
	} else {
		if err != nil {
			return err
		}
		c.logger().Debug("Found index template, not overwriting")
	}

	return nil
}

func (c *ESClient) getISMPolicyInfo() ISMPolicyInfo {
	var response ISMPolicyInfo
	response.SeqNo = -1
	response.PrimaryTerm = -1
	response.RolloverMinSize = ""
	response.err = nil

	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/_plugins/_ism/policies/rollover_delete_policy") //TODO: can't we use a client here?
	if resp.StatusCode() == http.StatusNotFound {
		return response // no policy there
	}
	if err != nil {
		response.err = fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body()) // there was an error with the request
		return response
	}

	var policy ISMPolicy
	err = json.Unmarshal(resp.Body(), &policy)
	if err != nil {
		response.err = err
		return response
	}

	response.SeqNo = int64(policy.SeqNo)
	response.PrimaryTerm = int64(policy.PrimaryTerm)

	foundRollover := false //we stop looking when we found the first rollover condition
	for _, state := range policy.Policy.States {
		for _, action := range state.Actions {
			if action.Rollover != nil {
				response.RolloverMinSize = action.Rollover.MinSize
				foundRollover = true
				break
			}
		}
		if foundRollover {
			break
		}
	}

	return response
}

// add/update rollover policy if necessary
func (c *ESClient) updateISMPolicy(numberOfShards int) error {
	// compute min_size based on the number of shards
	numberOfGb := numberOfShards * 10 //TODO configurable?
	minSize := fmt.Sprintf("%dgb", numberOfGb)

	//check if the policy is already there. If it is, we need the _seq_no and _primary_term
	policyInfo := c.getISMPolicyInfo()

	if policyInfo.err != nil {
		return policyInfo.err
	}

	if policyInfo.RolloverMinSize == minSize {
		c.logger().Debugf("ISM Policy already rotates at %s, so we skip updating it", minSize)
		return nil
	}

	versionInfo := ""

	if policyInfo.SeqNo != -1 && policyInfo.PrimaryTerm != -1 {
		versionInfo = fmt.Sprintf("?if_seq_no=%d&if_primary_term=%d",
			policyInfo.SeqNo, policyInfo.PrimaryTerm)
	}

	//TODO read "meat" of the policy from a file

	c.logger().Infof("Updating ISM policy with min_size %s, sequence number %d and primary term %d",
		minSize, policyInfo.SeqNo, policyInfo.PrimaryTerm)

	resp, err := resty.New().R().
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
								"min_index_age": "7d"
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
		c.logger().Errorf("Error while updating rollover policy: %s", err.Error())
		return err
	}
	if resp.StatusCode() != http.StatusCreated && resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}

	return nil
}

func (c *ESClient) getShardsOfWriteIndex() (int, error) {
	aliasName := "logstash_write" //TODO this should loop through everything we manage
	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/" + aliasName)
	if err != nil {
		return -1, err
	}
	if resp.StatusCode() != http.StatusOK {
		return -1, fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}

	var indices map[string]interface{}
	err = json.Unmarshal(resp.Body(), &indices)
	if err != nil {
		return -1, err
	}

	currentNumberOfShards := -1
	for indexName := range indices {
		if strings.HasPrefix(indexName, "logstash-") { //TODO this would be the index pattern
			indexObject := indices[indexName].(map[string]interface{})
			aliasesObject, ok := indexObject["aliases"].(map[string]interface{})
			if ok { //TODO at this point it feels like we need a nicer JSON parser
				aliasObject, ok := aliasesObject[aliasName].(map[string]interface{})
				if ok {
					writeIndexFound, ok := aliasObject["is_write_index"].(bool)
					if ok {
						if writeIndexFound {
							settingsObject, ok := indexObject["settings"].(map[string]interface{})
							if ok {
								settingsIndexObject, ok := settingsObject["index"].(map[string]interface{})
								if ok {
									currentNumberOfShardsString, ok := settingsIndexObject["number_of_shards"].(string)
									if ok {
										currentNumberOfShards, err = strconv.Atoi(currentNumberOfShardsString)
										if err != nil {
											return -1, fmt.Errorf("can't convert %s number_of_shards to int", currentNumberOfShardsString)
										}
										break
									} else {
										return -1, fmt.Errorf("can't find number_of_shards for %s", indexName)
									}
								} else {
									return -1, fmt.Errorf("can't find the 'index' setting key for %s", indexName)
								}
							} else {
								return -1, fmt.Errorf("can't find 'settings' key for %s", indexName)
							}
						}
					} else {
						log.Debugf("can't find %s key of the %s index", "is_write_index", indexName)
					}
				} else {
					log.Debugf("can't find %s key of the %s index", aliasName, indexName)
				}
			} else {
				log.Debugf("can't find %s key of the %s index", "aliases", indexName)
			}
		}
	}

	return currentNumberOfShards, nil

}

func (c *ESClient) getFirstIndexExists() (bool, error) {
	resp, err := resty.New().R().
		Get(c.Endpoint.String() + "/_aliases")
	if err != nil {
		return false, err
	}
	if resp.StatusCode() != http.StatusOK {
		return false, fmt.Errorf("code status %d - %s", resp.StatusCode(), resp.Body())
	}

	var aliases map[string]interface{}
	err = json.Unmarshal(resp.Body(), &aliases)
	if err != nil {
		return false, err
	}

	indexFound := false
	for indexName := range aliases {
		if strings.HasPrefix(indexName, "logstash-") { //TODO this would be the index pattern
			indexFound = true
			break
		}
	}

	return indexFound, nil
}

// in order to start indexing and do rollover, we have to have the first index
func (c *ESClient) createFirstIndex() error {
	c.logger().Infof("Didn't find Logstash index, will create one...")
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

	return nil
}

func (esSettings *ESSettings) updateExcludeIps(ips string) {
	esSettings.Persistent.Cluster.Routing.Allocation.Exclude.IP = null.StringFromPtr(&ips)
}

func (c *ESClient) updateTemplatesPolicyAndRollover(dataNodes int) error {
	// when we're done scaling up/down, we need to:
	// - update the component template to change the number of shards
	// - update the ISM policy's min_size value accordingly
	// - force rollover, so we use the new number of shards

	err := c.addMainComponentTemplate()
	if err != nil {
		return err
	}

	// even number of nodes? use half as the number of shards, because we have replicas
	// TODO: make this work with arbitrary number of replicas
	numberOfShards := dataNodes
	if dataNodes%2 == 0 {
		numberOfShards = dataNodes / 2
	}

	//TODO only if number of shards is different now
	err = c.updateShardsComponentTemplate(numberOfShards)
	if err != nil {
		return err
	}

	//TODO maintain sane total_shards_per_node values, too
	err = c.addTemplate()
	if err != nil {
		return err
	}

	err = c.updateISMPolicy(numberOfShards)
	if err != nil {
		return err
	}

	indexFound, err := c.getFirstIndexExists()
	if err != nil {
		return err
	}

	if !indexFound {
		err = c.createFirstIndex()
		if err != nil {
			return err
		}
	} else {
		currentNumberOfShards, err := c.getShardsOfWriteIndex()
		if err != nil {
			c.logger().Errorf("Failed to get current number of shards for the write alias: %s", err.Error())
			return err
		}

		c.logger().Infof("For alias: %s we have %d shards and we want %d shards",
			"logstash_write", currentNumberOfShards, numberOfShards)
		if currentNumberOfShards != numberOfShards {
			c.logger().Infof("Forcing rollover for alias: %s to change from %d to %d shards",
				"logstash_write", currentNumberOfShards, numberOfShards)
			err = c.forceRollover()
			if err != nil {
				return err
			}
		}
	}

	return nil
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
