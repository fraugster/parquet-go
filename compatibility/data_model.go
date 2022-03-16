//go:build ignore
// +build ignore

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
)

type sampleData struct {
	ID       string `json:"id"`
	Index    int64  `json:"index"`
	GUID     string `json:"guid"`
	IsActive bool   `json:"is_active"`
	Balance  string `json:"balance"`
	Picture  string `json:"picture"`
	Age      int32  `json:"age"`
	EyeColor string `json:"eye_color"`
	Name     struct {
		First string `json:"first"`
		Last  string `json:"last"`
	} `json:"name"`
	Company    string   `json:"company"`
	Email      string   `json:"email"`
	Phone      string   `json:"phone"`
	Address    string   `json:"address"`
	About      string   `json:"about"`
	Registered string   `json:"registered"`
	Latitude   float64  `json:"latitude"`
	Longitude  float64  `json:"longitude"`
	Tags       []string `json:"tags"`
	Range      []int32  `json:"range"`
	Friends    []struct {
		ID   int32  `json:"id"`
		Name string `json:"name"`
	} `json:"friends"`
	Greeting      string `json:"greeting"`
	FavoriteFruit string `json:"favorite_fruit"`
}

// The json library converts all numbers to float64. this is not good, I need to translate them to
// proper type
func (m sampleData) toMap() map[string]any {
	ret := map[string]any{
		"id":        []byte(m.ID),
		"index":     m.Index,
		"guid":      []byte(m.GUID),
		"is_active": m.IsActive,
		"balance":   []byte(m.Balance),
		"picture":   []byte(m.Picture),
		"age":       m.Age,
		"eye_color": []byte(m.EyeColor),
		"name": map[string]any{
			"first": []byte(m.Name.First),
			"last":  []byte(m.Name.Last),
		},

		"company":        []byte(m.Company),
		"email":          []byte(m.Email),
		"phone":          []byte(m.Phone),
		"address":        []byte(m.Address),
		"about":          []byte(m.About),
		"registered":     []byte(m.Registered),
		"latitude":       m.Latitude,
		"longitude":      m.Longitude,
		"greeting":       []byte(m.Greeting),
		"favorite_fruit": []byte(m.FavoriteFruit),
		"range":          m.Range,
	}

	var tags [][]byte
	for i := range m.Tags {
		tags = append(tags, []byte(m.Tags[i]))
	}

	ret["tags"] = tags

	var friends []map[string]any
	for i := range m.Friends {
		friends = append(friends, map[string]any{
			"id":   m.Friends[i].ID,
			"name": []byte(m.Friends[i].Name),
		})
	}

	ret["friends"] = friends
	return ret
}

var schema = `
message sample_data {
	required binary id (STRING);
	optional int64 index;
	optional binary guid (STRING);
	optional boolean is_active;
	optional binary balance (STRING);
	optional binary picture (STRING); 
	optional int32 age; 
	optional binary eye_color (STRING);
	optional group name {
		optional binary first (STRING);
		optional binary last (STRING);
	}
	optional binary company (STRING);
	optional binary email (STRING);
	optional binary phone (STRING);
	optional binary address (STRING);
	optional binary about (STRING);
	optional binary registered (STRING);
	optional double latitude;
	optional double longitude;
	repeated binary tags (STRING);
	repeated int32 range;
	repeated group friends {
		optional int32 id;
		optional binary name (STRING);
	}
	optional binary greeting (STRING);
	optional binary favorite_fruit (STRING); 
}
`

func loadDataFromJson(fl string) ([]*sampleData, error) {
	f, err := os.Open(fl)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var data []*sampleData
	if err := json.NewDecoder(f).Decode(&data); err != nil {
		return nil, err
	}

	return data, nil
}

func loadDataFromParquet(file string) ([]*sampleData, error) {
	cmd := exec.Command("java", "-jar", "/parquet-tools.jar", "cat", "--json", file)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	dec := json.NewDecoder(&out)

	var result []*sampleData
	for {
		var data sampleData
		if err := dec.Decode(&data); err != nil {
			if err == io.EOF {
				return result, nil
			}

			return nil, err
		}

		result = append(result, &data)
	}
}
