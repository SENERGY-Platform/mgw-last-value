/*
 * Copyright 2022 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pkg

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/mgw-last-value/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-last-value/pkg/mqtt"
	"log"
	"strings"
	"time"
)

type Storage interface {
	Set(key string, value []byte) error
	Get(key string) (value []byte, time *time.Time, err error)
}

func Worker(ctx context.Context, config configuration.Config, storage Storage) error {
	client, err := mqtt.New(ctx, config.MqttBroker, config.MqttClientId, config.MqttUser, config.MqttPw)
	if err != nil {
		return err
	}
	err = client.Subscribe("event/#", 2, func(topic string, payload []byte) {
		topicParts := strings.Split(topic, "/")
		if len(topicParts) != 3 {
			log.Println("WARNING: consumed invalid event topic", topic)
			return
		}
		deviceKey := topicParts[1]
		serviceKey := topicParts[2]
		key := deviceKey + "." + serviceKey
		if config.Debug {
			log.Println("DEBUG: store", key, string(payload))
		}
		err = storage.Set(key, payload)
		if len(topicParts) != 3 {
			log.Println("ERROR: unable to store value", err)
		}
	})
	if err != nil {
		return err
	}
	err = client.Subscribe("response/#", 2, func(topic string, response []byte) {
		topicParts := strings.Split(topic, "/")
		if len(topicParts) != 3 {
			log.Println("WARNING: consumed invalid event topic", topic)
			return
		}
		deviceKey := topicParts[1]
		serviceKey := topicParts[2]
		key := deviceKey + "." + serviceKey

		resp := Response{}
		err = json.Unmarshal(response, &resp)
		if err != nil {
			log.Println("WARNING: unexpected message in response topic:", string(response))
			return
		}

		if config.Debug {
			log.Println("DEBUG: store", key, resp.Data)
		}
		err = storage.Set(key, []byte(resp.Data))
		if len(topicParts) != 3 {
			log.Println("ERROR: unable to store value", err)
		}
	})
	if err != nil {
		return err
	}
	return nil
}

type Response struct {
	CommandId string `json:"command_id"`
	Data      string `json:"data"`
}
