package main

import (
	"encoding/base64"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	matcher "github.com/fogcloud-io/routermatcher"
	jsoniter "github.com/json-iterator/go"
)

type CloudGtwUplinkReq struct {
	RawTopic   string `json:"raw_topic"`
	RawPayload string `json:"raw_payload"`
	DeviceName string `json:"device_name"`
	ProductKey string `json:"product_key"`
	DeviceId   string `json:"device_id"`
}

var (
	uplinkMatcher matcher.Matcher

	ErrInvalidTopic       = errors.New("invalid topic")
	ErrNullTopicOrPayload = errors.New("null topic or payload")
	ErrInvalidUsername    = errors.New("invalid username")
)

func init() {
	uplinkMatcher = matcher.NewMqttTopicMatcher()

	uplinkMatcher.AddPath(FogTopicThingModelPropPost)
	uplinkMatcher.AddPath(FogTopicThingModelEventPost)
	uplinkMatcher.AddPath(FogTopicThingModelSvcReply)
	uplinkMatcher.AddPath(AliyunTopicThingModelPropPost)
	uplinkMatcher.AddPath(AliyunTopicThingModelEventPost)
}

func Handler(w http.ResponseWriter, r *http.Request) {
	req := new(CloudGtwUplinkReq)
	reqBytes, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	jsoniter.Unmarshal(reqBytes, req)

	topic, payload, err := HandleUplink(req.ProductKey, req.DeviceName, req.RawTopic, req.RawPayload)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	respBytes, _ := jsoniter.Marshal(struct {
		FogTopic   string `json:"fog_topic"`
		FogPayload string `json:"fog_payload"`
	}{
		FogTopic:   topic,
		FogPayload: payload,
	})
	w.Write(respBytes)
}

// 上行
const (
	FogTopicThingModelPropPost  = "fogcloud/+/+/thing/up/property/post"
	FogTopicThingModelEventPost = "fogcloud/+/+/thing/up/event/+/post"
	FogTopicThingModelSvcReply  = "fogcloud/+/+/thing/up/service/+/reply"

	AliyunTopicThingModelPropPost  = "/sys/+/+/thing/event/property/post"
	AliyunTopicThingModelEventPost = "/sys/+/+/thing/event/+/post"
)

func HandleUplink(productKey, deviceName, rawTopic, rawPayload string) (fogTopic, fogPayload string, err error) {
	if rawTopic == "" || rawPayload == "" {
		err = ErrNullTopicOrPayload
		return
	}
	log.Printf("raw_topic: %s, raw_payload: %s", rawTopic, rawPayload)

	matchedTopic, params, matched := uplinkMatcher.MatchWithAnonymousParams(fogTopic)
	if !matched {
		return "", "", ErrInvalidTopic
	}

	switch matchedTopic {
	case AliyunTopicThingModelPropPost:
		fogTopic = FillTopic(FogTopicThingModelPropPost, productKey, deviceName)
		fogPayload = payloadAliyunToFog(rawPayload, "")

	case AliyunTopicThingModelEventPost:
		if len(params) != 3 {
			err = ErrInvalidTopic
			return
		}
		fogTopic = FillTopic(FogTopicThingModelEventPost, productKey, deviceName, params[2])
		fogPayload = payloadAliyunToFog(rawPayload, "")
	}

	return
}

func payloadAliyunToFog(aliyunPayload string, method string) string {
	aliJson := new(AliyunJson)
	jsoniter.UnmarshalFromString(aliyunPayload, aliJson)

	fogJson := new(FogJson)
	fogJson.Version = aliJson.Version
	fogJson.Method = method
	fogJson.Params = aliJson.Params

	bytes, _ := jsoniter.Marshal(aliJson)
	return base64.StdEncoding.EncodeToString(bytes)
}

type FogJson struct {
	Id        uint32                 `json:"id"`
	Version   string                 `json:"version"`
	Method    string                 `json:"method,omitempty"`
	Timestamp int64                  `json:"timestamp"`
	Params    map[string]interface{} `json:"params"`
}

type AliyunJson struct {
	Id      string                 `json:"id"`
	Version string                 `json:"version"`
	Params  map[string]interface{} `json:"params"`
	Method  string                 `json:"method"`
}

func FillTopic(topic string, replaceStr ...string) string {
	s := topic
	for i := range replaceStr {
		s = strings.Replace(s, "+", replaceStr[i], 1)
	}
	return s
}
