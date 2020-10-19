package stalumi

import (
	"encoding/json"
	"fmt"

	"github.com/pulumi/pulumi/sdk/v2/go/common/resource"
)


func JSONToPropertyMap(data []byte) (resource.PropertyMap, error) {

	raw := make(map[string]interface{})

	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("error unmarshalling data: %v", err)
	}

	return resource.NewPropertyMapFromMap(raw), nil
}


func PropertyMapToJSON(data resource.PropertyMap) ([]byte, error) {

	var mapper func(resource.PropertyValue) (interface{}, bool)
	mapper = func(value resource.PropertyValue) (interface{}, bool) {
		switch v := value.V.(type) {
		case resource.PropertyMap:
			return v.MapRepl(nil, mapper), true
		}
		return value.V, true
	}

	simpleMap := data.MapRepl(nil, mapper)
	out, err := json.Marshal(simpleMap)
	if err != nil {
		return nil, fmt.Errorf("error marshalling property map: %v", err)
	}

	return out, nil
}
