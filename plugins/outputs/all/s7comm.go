//go:build !custom || outputs || outputs.s7comm

package all

import _ "github.com/influxdata/telegraf/plugins/outputs/s7comm" // register plugin
