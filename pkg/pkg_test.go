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
	"bytes"
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/mgw-last-value/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-last-value/pkg/mqtt"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/ory/dockertest/v3"
	"log"
	"math/rand"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLastValueApi(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config, err := configuration.Load("../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	config.BadgerLocation = t.TempDir()
	config.HttpPort, err = GetFreePort()
	if err != nil {
		t.Error(err)
		return
	}
	config.Debug = true
	config, err = mqttEnv(config, ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}
	err = Start(ctx, wg, config)
	if err != nil {
		t.Error(err)
		return
	}

	t.Run("send values to mqtt", func(t *testing.T) {
		client, err := mqtt.New(ctx, config.MqttBroker, "test-client", config.MqttUser, config.MqttPw)
		if err != nil {
			t.Error(err)
			return
		}
		err = client.Publish("event/d1/replace", 2, false, []byte(`13`))
		if err != nil {
			t.Error(err)
			return
		}
		err = client.Publish("event/d1/s0", 2, false, []byte(``))
		if err != nil {
			t.Error(err)
			return
		}
		err = client.Publish("event/d1/s1", 2, false, []byte(`{}`))
		if err != nil {
			t.Error(err)
			return
		}
		err = client.Publish("event/d1/s2", 2, false, []byte(`42`))
		if err != nil {
			t.Error(err)
			return
		}
		err = client.Publish("event/d1/s3", 2, false, []byte(`"foo"`))
		if err != nil {
			t.Error(err)
			return
		}
		err = client.Publish("event/d1/s4", 2, false, []byte(`bar`))
		if err != nil {
			t.Error(err)
			return
		}
		err = client.Publish("event/d1/s6", 2, false, []byte(`null`))
		if err != nil {
			t.Error(err)
			return
		}
		err = client.Publish("event/d1/s7", 2, false, []byte(`{"foo": "bar", "batz":42}`))
		if err != nil {
			t.Error(err)
			return
		}
		err = client.Publish("event/d1/s8", 2, false, []byte(`[42, "foo", {"batz":13}]`))
		if err != nil {
			t.Error(err)
			return
		}
		err = client.Publish("event/d1/s9", 2, false, []byte(`{"foo":[42, "bar"]}`))
		if err != nil {
			t.Error(err)
			return
		}
		err = client.Publish("event/d1/replace", 2, false, []byte(`42`))
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(1 * time.Second)

	t.Run("query", func(t *testing.T) {
		t.Run(queryTest(config, "d1", "s0", "", "", true))
		t.Run(queryTest(config, "unknown", "s1", "", nil, false))
		t.Run(queryTest(config, "d1", "s1", "", map[string]interface{}{}, true))
		t.Run(queryTest(config, "d1", "s2", "", float64(42), true))
		t.Run(queryTest(config, "d1", "s2", "foo", nil, true))
		t.Run(queryTest(config, "d1", "s3", "", "foo", true))
		t.Run(queryTest(config, "d1", "s4", "", "bar", true))
		t.Run(queryTest(config, "d1", "s6", "", nil, true))
		t.Run(queryTest(config, "d1", "s7", "", map[string]interface{}{"foo": "bar", "batz": float64(42)}, true))
		t.Run(queryTest(config, "d1", "s7", "foo", "bar", true))
		t.Run(queryTest(config, "d1", "s7", "batz", float64(42), true))
		t.Run(queryTest(config, "d1", "s7", "bar", nil, true))
		t.Run(queryTest(config, "d1", "s8", "0", float64(42), true))
		t.Run(queryTest(config, "d1", "s8", "1", "foo", true))
		t.Run(queryTest(config, "d1", "s8", "2.batz", float64(13), true))
		t.Run(queryTest(config, "d1", "s9", "foo.0", float64(42), true))
		t.Run(queryTest(config, "d1", "s9", "foo.1", "bar", true))
		t.Run(queryTest(config, "d1", "replace", "", float64(42), true))
	})
}

func queryTest(config configuration.Config, deviceKey string, serviceKey string, path string, expected interface{}, expectTime bool) (testName string, f func(t *testing.T)) {
	return strings.Join([]string{deviceKey, serviceKey, path}, "."), func(t *testing.T) {
		type LastValueRequest struct {
			DeviceId   string
			ServiceId  string
			ColumnName string
		}

		type LastValueResponse struct {
			Time  *string     `json:"time"`
			Value interface{} `json:"value"`
		}

		buff := &bytes.Buffer{}
		err := json.NewEncoder(buff).Encode([]LastValueRequest{{
			DeviceId:   deviceKey,
			ServiceId:  serviceKey,
			ColumnName: path,
		}})
		if err != nil {
			t.Error(err)
			return
		}
		resp, err := http.Post("http://localhost:"+config.HttpPort+"/last-values", "application/json", buff)
		if err != nil {
			t.Error(err)
			return
		}
		if resp.StatusCode != 200 {
			t.Error(resp.StatusCode)
			return
		}
		result := []LastValueResponse{}
		err = json.NewDecoder(resp.Body).Decode(&result)
		if err != nil {
			t.Error(err)
			return
		}
		t.Log(result)
		if len(result) != 1 {
			t.Error(result)
			return
		}
		if (result[0].Time != nil) != expectTime {
			t.Error("\n", result[0].Time, "\n", expectTime)
			if result[0].Time != nil {
				t.Error(*result[0].Time)
			}
		}
		if !reflect.DeepEqual(result[0].Value, expected) {
			t.Error("\n", result[0].Value, "\n", expected)
			return
		}
	}
}

func mqttEnv(config configuration.Config, ctx context.Context, wg *sync.WaitGroup) (configuration.Config, error) {
	mqttPort, _, err := Mqtt(ctx, wg)
	if err != nil {
		return config, err
	}
	config.MqttBroker = "tcp://localhost:" + mqttPort
	return config, nil
}

func Mqtt(ctx context.Context, wg *sync.WaitGroup) (hostPort string, ipAddress string, err error) {
	log.Println("start mqtt broker")
	pool, err := dockertest.NewPool("")
	if err != nil {
		return "", "", err
	}
	container, err := pool.Run("eclipse-mosquitto", "1.6.12", []string{})
	if err != nil {
		return "", "", err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("DEBUG: remove container " + container.Container.Name)
		log.Println(container.Close())
	}()
	//go Dockerlog(pool, ctx, container, "MQTT-BROKER")
	hostPort = container.GetPort("1883/tcp")
	err = pool.Retry(func() error {
		log.Println("try to connection to broker...")
		options := paho.NewClientOptions().
			SetAutoReconnect(true).
			SetCleanSession(false).
			SetClientID("try-test-connection-" + strconv.Itoa(rand.Int())).
			AddBroker("tcp://localhost:" + hostPort)

		client := paho.NewClient(options)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Println("Error on Mqtt.Connect(): ", token.Error())
			return token.Error()
		}
		defer client.Disconnect(0)
		return nil
	})
	return hostPort, container.Container.NetworkSettings.IPAddress, err
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func GetFreePort() (string, error) {
	temp, err := getFreePort()
	return strconv.Itoa(temp), err
}