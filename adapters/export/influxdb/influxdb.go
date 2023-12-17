package influxdb

import (
  "log"
  "time"
  "context"
  "errors"
  "strconv"
  "reflect"
  
  sofar "github.com/kubaceg/sofar_g3_lsw3_logger_reader/adapters/devices/sofar"

  influxdb2 "github.com/influxdata/influxdb-client-go/v2"
  "github.com/influxdata/influxdb-client-go/v2/api"
  "github.com/influxdata/influxdb-client-go/v2/api/write"
)

type InfluxDBConfig struct {
	Url       string  `yaml:"url"`
	Token     string  `yaml:"token"`
	Org       string  `yaml:"org"`
	Bucket    string  `yaml:"bucket"`
}

type Influx struct {
	client      influxdb2.Client
	writeAPI    api.WriteAPIBlocking
	measurement string
}

func New(config *InfluxDBConfig) (*Influx, error) {
	if config.Url == "" {
		return nil, errors.New("missing URL endpoint")
	}
	if config.Token == "" {
		return nil, errors.New("missing token")
	}
	if config.Org == "" {
		return nil, errors.New("missing organization")
	}
	if config.Bucket == "" {
		return nil, errors.New("missing bucket")
	}

	influx := &Influx{}
	influx.client = influxdb2.NewClient(config.Url, config.Token)
	influx.writeAPI = influx.client.WriteAPIBlocking(config.Org, config.Bucket)
	influx.measurement = "Sofar Logger"
	
	// validate client connection health
    resp, err := influx.client.Health(context.Background())
	log.Printf("Influx Client ready: %s", resp.Status)

	return influx, err
}

func (influx *Influx) InsertRecord(m map[string]interface{}) error {
	var sysState string
	for k, v := range m {
		if k == "SysState" {
			switch v.(uint16) {
				case 0:
					sysState = "Waiting/Normal State"
				case 1:
					sysState = "Detection State"
				case 2:
					sysState = "Grid-connected State"
				case 3:
					sysState = "Emergency Power Supply State"
				case 4:
					sysState = "Recoverable Fault State"
				case 5:
					sysState = "Permanent Fault State"
				case 6:
					sysState = "Upgrade State"
				case 7:
					sysState = "Self-charging State"
				default:
			}
		}
	}
	for k, v := range m {
		if k == "SysState" {
			continue
		}
		value := v
		rrName, rrDevice, unit, scalingFactor := sofar.GetMatchFromFullList(k)
		
		valueName := k
		if unit != "" {
			valueName = valueName + "_" + unit
		}
		
		if scalingFactor != "" {
			// Apply scaling factor to value
			sf, err := strconv.ParseFloat(scalingFactor, 64)
			if err == nil {
				if temp, err2 := convertToInt(v); err2 == nil {
					value = float64(temp) * sf
				} else {
					log.Printf("Error on conversion: %s", err2)
				}
			}
		}
		
		point := write.NewPoint(influx.measurement, map[string]string{"device": rrDevice, "metric-name": rrName, "system-state": sysState},  map[string]interface{}{valueName: value}, time.Now())

		if err := influx.writeAPI.WritePoint(context.Background(), point); err != nil {
			return err
		}
	}
	
	return nil
}

func convertToInt(input interface{}) (int, error) {
	switch input.(type) {
		case uint16:
			v, _ := input.(uint16)
			return int(v), nil
		case uint32:
			v, _ := input.(uint32)
			return int(v), nil
		case int16:
			v, _ := input.(uint16)
			return int(v), nil
		case int32:
			v, _ := input.(uint32)
			return int(v), nil
		default:
			return 0, errors.New("unknown type: " + reflect.TypeOf(input).String())
	}
}