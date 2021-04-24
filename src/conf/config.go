package conf

import (
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	flag "github.com/spf13/pflag"

	"errors"
	"strings"
	"time"
)

// The Config structure
type Config struct {
	k     *koanf.Koanf          // koanf instance
	dl    string                // delimiter string
	files map[string]*file.File // map of loaded files
}

// New creates a new Config object
func New() *Config {
	var c = Config{
		dl:    ".",
		k:     koanf.New("."),
		files: make(map[string]*file.File),
	}
	return &c
}

// LoadJSON LoadJson loads json config file
func (c *Config) LoadJSON(f string) error {
	p := file.Provider(f)
	c.files[f] = p
	return c.k.Load(p, json.Parser())
}

// LoadYaml loads yaml config file
func (c *Config) LoadYaml(f string) error {
	p := file.Provider(f)
	c.files[f] = p
	return c.k.Load(p, yaml.Parser())
}

// LoadFlag loads config from flags
func (c *Config) LoadFlag(f *flag.FlagSet) error {
	return c.k.Load(posflag.Provider(f, c.dl, c.k), nil)
}

// LoadDefault loads default values from map
// eg
// cfg.LoadDefault(map[string]interface{}{
//			"com":                 "COM4",
//			"reset":               false,
//			"path":                "/tmp",
//			"timeperiod":          60,
//			"debug.active":        false,
//			"debug.path":          "/tmp",
//			"websserver.active":   true,
//			"websserver.port":     ":8080",
//			"webservice.version":  true,
//			"webservice.winsol":   true,
//			"webservice.data":     true,
//			"webservice.download": true,
//		})
func (c *Config) LoadDefault(m map[string]interface{}) error {
	return c.k.Load(confmap.Provider(m, c.dl), nil)
}

// LoadEnv loads config rom env variables
// Load environment variables and merge into the loaded config.
// pfx is the prefix to filter the env vars by.
// c.dl is the delimiter used to represent the key hierarchy in env vars.
// The (optional, or can be nil) function can be used to transform
// the env var names, for instance, to lowercase them.
//
// For example, env vars: MYVAR_TYPE and MYVAR_PARENT1_CHILD1_NAME
// will be merged into the "type" and the nested "parent1.child1.name"
// keys in the config file here as we lowercase the key,
// replace `_` with `.` and strip the MYVAR_ prefix so that
// only "parent1.child1.name" remains.
func (c *Config) LoadEnv(pfx string) error {
	return c.k.Load(env.Provider(pfx, c.dl, func(s string) string {
		return strings.ReplaceAll(strings.ToLower(
			strings.TrimPrefix(s, pfx)), "_", c.dl)
	}), nil)
}

func (c *Config) watch(f string, onReloaded func(), parser koanf.Parser) (err error) {
	p, ok := c.files[f]
	if !ok {
		return errors.New("file not loaded")
	}
	err = p.Watch(func(event interface{}, err2 error) {
		if err2 != nil {
			err = err2
			return
		}
		err = c.k.Load(p, parser)
		if err != nil {
			onReloaded()
		}
	})
	return
}

// WatchJSON WatchJson watch loaded json config file for changes, reloads it when changed and triggers onReloaded callback
func (c *Config) WatchJSON(f string, onReloaded func()) error {
	return c.watch(f, onReloaded, json.Parser())
}

// WatchYaml watch loaded yaml config file for changes, reloads it when changed and triggers onReloaded callback
func (c *Config) WatchYaml(f string, onReloaded func()) error {
	return c.watch(f, onReloaded, yaml.Parser())
}

// Keys returns the list of flattened key paths that can be used to access config values
func (c *Config) Keys() []string {
	return c.k.Keys()
}

// KeyMap returns a map of all possible key path combinations possible in the loaded nested conf map
func (c *Config) KeyMap() map[string][]string {
	return c.k.KeyMap()
}

// All returns a flat map of flattened key paths and their corresponding config values
func (c *Config) All() map[string]interface{} {
	return c.k.All()
}

// Print prints a human readable copy of the flattened key paths and their values for debugging
func (c *Config) Print() *Config {
	c.k.Print()
	return c
}

// Sprint returns a human readable copy of the flattened key paths and their values for debugging
func (c *Config) Sprint() string {
	return c.k.Sprint()
}

// Cut cuts the loaded nested conf map at the given path and returns a new Koanf instance with the children
func (c *Config) Cut(path string) *Config {
	return &Config{
		dl: c.dl,
		k:  c.k.Cut(path),
	}
}

// Merge merges the config map of a Config instance into the current instance
func (c *Config) Merge(config *Config) *Config {
	c.k.Merge(config.k)
	return c
}

// Unmarshal scans the given nested key path into a given struct (like json.Unmarshal)
func (c *Config) Unmarshal(path string, out interface{}) error {
	return c.k.Unmarshal(path, out)
}

// Exists returns true if the given key path exists in the conf map
func (c *Config) Exists(path string) bool {
	return c.k.Exists(path)
}

// Get returns the value for the given key path, and if it doesnt exist, returns nil
func (c *Config) Get(path string) interface{} {
	return c.k.Get(path)
}

// Int64 returns the int64 value of a given key path or 0 if the path
// does not exist or if the value is not a valid int64.
func (c *Config) Int64(path string) int64 {
	return c.k.Int64(path)
}

// Int64s returns the []int64 slice value of a given key path or an
// empty []int64 slice if the path does not exist or if the value
// is not a valid int slice.
func (c *Config) Int64s(path string) []int64 {
	return c.k.Int64s(path)
}

// Int64Map returns the map[string]int64 value of a given key path
// or an empty map[string]int64 if the path does not exist or if the
// value is not a valid int64 map.
func (c *Config) Int64Map(path string) map[string]int64 {
	return c.k.Int64Map(path)
}

// Int returns the int value of a given key path or 0 if the path
// does not exist or if the value is not a valid int.
func (c *Config) Int(path string) int {
	return c.k.Int(path)
}

// Ints returns the []int slice value of a given key path or an
// empty []int slice if the path does not exist or if the value
// is not a valid int slice.
func (c *Config) Ints(path string) []int {
	return c.k.Ints(path)
}

// IntMap returns the map[string]int value of a given key path
// or an empty map[string]int if the path does not exist or if the
// value is not a valid int map.
func (c *Config) IntMap(path string) map[string]int {
	return c.k.IntMap(path)
}

// Float64 returns the float64 value of a given key path or 0 if the path
// does not exist or if the value is not a valid float64.
func (c *Config) Float64(path string) float64 {
	return c.k.Float64(path)
}

// Float64s returns the []float64 slice value of a given key path or an
// empty []float64 slice if the path does not exist or if the value
// is not a valid float64 slice.
func (c *Config) Float64s(path string) []float64 {
	return c.k.Float64s(path)
}

// Float64Map returns the map[string]float64 value of a given key path
// or an empty map[string]float64 if the path does not exist or if the
// value is not a valid float64 map.
func (c *Config) Float64Map(path string) map[string]float64 {
	return c.k.Float64Map(path)
}

// Duration returns the time.Duration value of a given key path assuming
// that the key contains a valid numeric value.
func (c *Config) Duration(path string) time.Duration {
	return c.k.Duration(path)
}

// Time attempts to parse the value of a given key path and return time.Time
// representation. If the value is numeric, it is treated as a UNIX timestamp
// and if it's string, a parse is attempted with the given layout.
func (c *Config) Time(path, layout string) time.Time {
	return c.k.Time(path, layout)
}

// String returns the string value of a given key path or "" if the path
// does not exist or if the value is not a valid string.
func (c *Config) String(path string) string {
	return c.k.String(path)
}

// Strings returns the []string slice value of a given key path or an
// empty []string slice if the path does not exist or if the value
// is not a valid string slice.
func (c *Config) Strings(path string) []string {
	return c.k.Strings(path)
}

// StringMap returns the map[string]string value of a given key path
// or an empty map[string]string if the path does not exist or if the
// value is not a valid string map.
func (c *Config) StringMap(path string) map[string]string {
	return c.k.StringMap(path)
}

// Bytes returns the []byte value of a given key path or an empty
// []byte slice if the path does not exist or if the value is not a valid string.
func (c *Config) Bytes(path string) []byte {
	return c.k.Bytes(path)
}

// Bool returns the bool value of a given key path or false if the path
// does not exist or if the value is not a valid bool representation.
// Accepted string representations of bool are the ones supported by strconv.ParseBool.
func (c *Config) Bool(path string) bool {
	return c.k.Bool(path)
}

// Bools returns the []bool slice value of a given key path or an
// empty []bool slice if the path does not exist or if the value
// is not a valid bool slice.
func (c *Config) Bools(path string) []bool {
	return c.k.Bools(path)
}

// BoolMap returns the map[string]bool value of a given key path
// or an empty map[string]bool if the path does not exist or if the
// value is not a valid bool map.
func (c *Config) BoolMap(path string) map[string]bool {
	return c.k.BoolMap(path)
}

// MapKeys returns a sorted string list of keys in a map addressed by the
// given path. If the path is not a map, an empty string slice is
// returned.
func (c *Config) MapKeys(path string) []string {
	return c.k.MapKeys(path)
}
