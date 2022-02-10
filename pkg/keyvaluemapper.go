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
	"encoding/json"
	"log"
	"strconv"
	"strings"
)

type KeyValueMapperImpl struct {
	Debug bool
}

func (this KeyValueMapperImpl) Get(message []byte) (result map[string]interface{}) {
	var value interface{}
	err := json.Unmarshal(message, &value)
	if err != nil {
		if this.Debug {
			log.Println("WARNING: message is not json --> return {\"\":message}")
		}
		return map[string]interface{}{"": string(message)}
	}
	return this.walk([]string{}, value)
}

func (this KeyValueMapperImpl) walk(path []string, value interface{}) (result map[string]interface{}) {
	result = map[string]interface{}{
		strings.Join(path, "."): value,
	}
	switch v := value.(type) {
	case map[string]interface{}:
		for k, sub := range v {
			for subPath, subValue := range this.walk(append(path, k), sub) {
				result[subPath] = subValue
			}
		}
	case []interface{}:
		for k, sub := range v {
			for subPath, subValue := range this.walk(append(path, strconv.Itoa(k)), sub) {
				result[subPath] = subValue
			}
		}
	}
	return result
}
