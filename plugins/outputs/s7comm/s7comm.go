package s7comm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/robinson/gos7"
)

var (
	regexAddr = regexp.MustCompile(addressRegexp)
	areaMap   = map[string]int{
		"PE": 0x81, // process inputs
		"PA": 0x82, // process outputs
		"MK": 0x83, // Merkers
		"DB": 0x84, // DB
		"C":  0x1C, // counters
		"T":  0x1D, // timers
	}
	wordLenMap = map[string]int{
		"X":  0x01, // Bit
		"B":  0x02, // Byte (8 bit)
		"C":  0x03, // Char (8 bit)
		"S":  0x03, // String (8 bit)
		"W":  0x04, // Word (16 bit)
		"I":  0x05, // Integer (16 bit)
		"DW": 0x06, // Double Word (32 bit)
		"DI": 0x07, // Double integer (32 bit)
		"LI": 0x06, // Long integer (64 bit)
		"R":  0x08, // IEEE 754 real (32 bit)
		"LR": 0x06, // IEEE 754 double (64-bit)
		"DT": 0x0F, // Date and time (7 byte)
	}
)

const addressRegexp = `^(?P<area>[A-Z]+)(?P<no>[0-9]+)\.(?P<type>[A-Z]+)(?P<start>[0-9]+)(?:\.(?P<extra>.*))?$`

// 配置结构

type MetricDefinition struct {
	Name   string                  `toml:"name"`
	Fields []MetricFieldDefinition `toml:"fields"`
	Tags   map[string]string       `toml:"tags"`
}

type MetricFieldDefinition struct {
	Name    string `toml:"name"`
	Address string `toml:"address"`
}

// S7Comm is the main struct for the plugin
type S7Comm struct {
	Address      string             `toml:"address"`
	Rack         int                `toml:"rack"`
	Slot         int                `toml:"slot"`
	Timeout      config.Duration    `toml:"timeout"`
	MaxBatchSize int                `toml:"max_batch_size"`
	Metrics      []MetricDefinition `toml:"metric"`

	client  gos7.Client
	handler *gos7.TCPClientHandler
}

// SampleConfig returns a sample configuration for the plugin
func (s *S7Comm) SampleConfig() string {
	return `
  address = "192.168.0.1:102" # PLC address
  rack = 0                    # PLC rack
  slot = 1                    # PLC slot
  timeout = "5s"              # Connection timeout
  max_batch_size = 18         # Maximum items per batch write (S7 protocol limit)

  [[outputs.s7comm.metric]]
    name = "s7comm"
    fields = [
      { name="rpm",        address="DB1.R4"    },
      { name="status_ok",  address="DB1.X2.1"  }
    ]
`
}

// Connect establishes a connection to the S7 PLC
func (s *S7Comm) Connect() error {
	handler := gos7.NewTCPClientHandler(s.Address, s.Rack, s.Slot)
	if s.Timeout > 0 {
		handler.Timeout = time.Duration(s.Timeout)
	}
	if err := handler.Connect(); err != nil {
		return fmt.Errorf("failed to connect to S7 PLC: %w", err)
	}
	s.handler = handler
	s.client = gos7.NewClient(handler)
	return nil
}

// Close closes the connection to the PLC
func (s *S7Comm) Close() error {
	if s.handler != nil {
		return s.handler.Close()
	}
	return nil
}

// address 解析和类型转换
func parseFieldAddress(address string) (*gos7.S7DataItem, string, error) {
	if !regexAddr.MatchString(address) {
		return nil, "", fmt.Errorf("invalid address %q", address)
	}
	names := regexAddr.SubexpNames()[1:]
	parts := regexAddr.FindStringSubmatch(address)[1:]
	groups := make(map[string]string, len(names))
	for i, n := range names {
		groups[n] = parts[i]
	}
	area, found := areaMap[groups["area"]]
	if !found {
		return nil, "", errors.New("invalid area")
	}
	wordlen, found := wordLenMap[groups["type"]]
	if !found {
		return nil, "", errors.New("unknown data type")
	}
	areaidx, err := strconv.Atoi(groups["no"])
	if err != nil {
		return nil, "", fmt.Errorf("invalid area index: %w", err)
	}
	start, err := strconv.Atoi(groups["start"])
	if err != nil {
		return nil, "", fmt.Errorf("invalid start address: %w", err)
	}
	var extra, bit int
	dtype := groups["type"]
	switch dtype {
	case "S":
		x := groups["extra"]
		if x == "" {
			return nil, "", errors.New("extra parameter required")
		}
		extra, err = strconv.Atoi(x)
		if err != nil {
			return nil, "", fmt.Errorf("invalid extra parameter: %w", err)
		}
		if extra < 1 {
			return nil, "", fmt.Errorf("invalid extra parameter %d", extra)
		}
	case "X":
		x := groups["extra"]
		if x == "" {
			return nil, "", errors.New("extra parameter required")
		}
		bit, err = strconv.Atoi(x)
		if err != nil {
			return nil, "", fmt.Errorf("invalid extra parameter: %w", err)
		}
		if bit < 0 || bit > 7 {
			return nil, "", fmt.Errorf("invalid extra parameter: bit address %d out of range", bit)
		}
	default:
		if groups["extra"] != "" {
			return nil, "", errors.New("extra parameter specified but not used")
		}
	}
	amount := 1
	var buflen int
	switch dtype {
	case "X", "B", "C":
		buflen = 1
	case "W", "I":
		buflen = 2
	case "DW", "DI", "R":
		buflen = 4
	case "LR", "LI":
		buflen = 8
		amount = 2
	case "DT":
		buflen = 7
	case "S":
		amount = extra
		buflen = extra + 2
	default:
		return nil, "", errors.New("invalid data type")
	}
	item := &gos7.S7DataItem{
		Area:     area,
		WordLen:  wordlen,
		Bit:      bit,
		DBNumber: areaidx,
		Start:    start,
		Amount:   amount,
		Data:     make([]byte, buflen),
	}
	return item, dtype, nil
}

// 类型转换: Go value -> S7 字节
func fillS7Data(dtype string, value interface{}, buf []byte) error {
	switch dtype {
	case "X":
		var b bool
		switch val := value.(type) {
		case bool:
			b = val
		case int:
			b = val != 0
		case int64:
			b = val != 0
		case float64:
			b = val != 0
		default:
			return fmt.Errorf("expect bool/int/int64/float64 for type X, got %T", value)
		}
		if b {
			buf[0] = 1
		} else {
			buf[0] = 0
		}
	case "B":
		var v uint8
		switch val := value.(type) {
		case uint8:
			v = val
		case int:
			v = uint8(val)
		case int64:
			v = uint8(val)
		case float64:
			v = uint8(val)
		default:
			return fmt.Errorf("expect uint8/int/int64/float64 for type B, got %T", value)
		}
		buf[0] = v
	case "C":
		str, ok := value.(string)
		if !ok || len(str) == 0 {
			return fmt.Errorf("expect string for type C")
		}
		buf[0] = str[0]
	case "S":
		str, ok := value.(string)
		if !ok {
			return fmt.Errorf("expect string for type S")
		}
		if len(buf) < 2 {
			return fmt.Errorf("buffer too small for S")
		}
		buf[0] = uint8(len(buf) - 2) // max len
		buf[1] = uint8(len(str))     // actual len
		copy(buf[2:], str)
	case "W":
		var v uint16
		switch val := value.(type) {
		case uint16:
			v = val
		case int:
			v = uint16(val)
		case int64:
			v = uint16(val)
		case float64:
			v = uint16(val)
		default:
			return fmt.Errorf("expect uint16/int/int64/float64 for type W, got %T", value)
		}
		binary.BigEndian.PutUint16(buf, v)
	case "I":
		var v int16
		switch val := value.(type) {
		case int16:
			v = val
		case int:
			v = int16(val)
		case int64:
			v = int16(val)
		case float64:
			v = int16(val)
		default:
			return fmt.Errorf("expect int16/int/int64/float64 for type I, got %T", value)
		}
		binary.BigEndian.PutUint16(buf, uint16(v))
	case "DW":
		var v uint32
		switch val := value.(type) {
		case uint32:
			v = val
		case int:
			v = uint32(val)
		case int64:
			v = uint32(val)
		case float64:
			v = uint32(val)
		default:
			return fmt.Errorf("expect uint32/int/int64/float64 for type DW, got %T", value)
		}
		binary.BigEndian.PutUint32(buf, v)
	case "DI":
		var v int32
		switch val := value.(type) {
		case int32:
			v = val
		case int:
			v = int32(val)
		case int64:
			v = int32(val)
		case float64:
			v = int32(val)
		default:
			return fmt.Errorf("expect int32/int/int64/float64 for type DI, got %T", value)
		}
		binary.BigEndian.PutUint32(buf, uint32(v))
	case "LI":
		var v int64
		switch val := value.(type) {
		case int64:
			v = val
		case int:
			v = int64(val)
		case float64:
			v = int64(val)
		default:
			return fmt.Errorf("expect int64/int/float64 for type LI, got %T", value)
		}
		binary.BigEndian.PutUint64(buf, uint64(v))
	case "R":
		var v float32
		switch val := value.(type) {
		case float32:
			v = val
		case float64:
			v = float32(val)
		case int:
			v = float32(val)
		case int64:
			v = float32(val)
		default:
			return fmt.Errorf("expect float32/float64/int/int64 for type R, got %T", value)
		}
		bits := math.Float32bits(v)
		binary.BigEndian.PutUint32(buf, bits)
	case "LR":
		var v float64
		switch val := value.(type) {
		case float64:
			v = val
		case float32:
			v = float64(val)
		case int:
			v = float64(val)
		case int64:
			v = float64(val)
		default:
			return fmt.Errorf("expect float64/float32/int/int64 for type LR, got %T", value)
		}
		bits := math.Float64bits(v)
		binary.BigEndian.PutUint64(buf, bits)
	case "DT":
		v, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("expect time.Time for type DT")
		}
		helper := &gos7.Helper{}
		helper.SetDateTimeAt(buf, 0, v)
	default:
		return fmt.Errorf("unsupported type: %s", dtype)
	}
	return nil
}

// Write: 将 metric 的 field 写入 S7
func (s *S7Comm) Write(metrics []telegraf.Metric) error {
	// 检查连接状态，如果连接为空或断开则重新连接
	if s.client == nil || s.handler == nil {
		if err := s.Connect(); err != nil {
			return fmt.Errorf("failed to connect to S7 PLC: %w", err)
		}
	}

	// 设置默认批量大小
	maxBatchSize := s.MaxBatchSize
	if maxBatchSize <= 0 {
		maxBatchSize = 18 // S7协议默认限制
	}

	for _, m := range metrics {
		for _, def := range s.Metrics {
			// 收集该metric的所有数据项
			var items []gos7.S7DataItem

			for _, field := range def.Fields {
				val, ok := m.GetField(field.Name)
				if !ok {
					continue
				}
				item, dtype, err := parseFieldAddress(field.Address)
				if err != nil {
					return err
				}
				if err := fillS7Data(dtype, val, item.Data); err != nil {
					return err
				}
				items = append(items, *item)
			}

			// 分批写入，确保不超过S7协议限制
			for i := 0; i < len(items); i += maxBatchSize {
				end := i + maxBatchSize
				if end > len(items) {
					end = len(items)
				}

				batch := items[i:end]
				if err := s.client.AGWriteMulti(batch, len(batch)); err != nil {
					// 如果写入失败，尝试重新连接并重试一次
					if err := s.Connect(); err != nil {
						return fmt.Errorf("failed to reconnect to S7 PLC: %w", err)
					}
					if err := s.client.AGWriteMulti(batch, len(batch)); err != nil {
						return fmt.Errorf("batch write to S7 failed (batch %d-%d) after reconnect: %w", i, end-1, err)
					}
				}
			}
		}
	}
	return nil
}

func init() {
	outputs.Add("s7comm", func() telegraf.Output { return &S7Comm{} })
}
