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

import "time"

type KeyValueMapper interface {
	Get(message []byte) map[string]interface{}
}

type Query struct {
	mapper KeyValueMapper
	db     Storage
}

func NewQuery(mapper KeyValueMapper, db Storage) *Query {
	return &Query{
		mapper: mapper,
		db:     db,
	}
}

func (this *Query) Get(deviceKey, serviceKey, path string) (value interface{}, time *time.Time, err error) {
	key := deviceKey + "." + serviceKey
	var tempVal []byte
	tempVal, time, err = this.db.Get(key)
	if err != nil {
		return value, time, err
	}
	mapped := this.mapper.Get(tempVal)
	value = mapped[path]
	return value, time, nil
}
