package main

import (
	"C"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "cos_parquet", "Fluent Bit COS Parquet Output Plugin")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	ctx, err := NewPluginContext(plugin)
	if err != nil {
		output.FLBPluginUnregister(plugin)
		return output.FLB_ERROR
	}

	output.FLBPluginSetContext(plugin, ctx)
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	pluginCtx := output.FLBPluginGetContext(ctx).(*PluginContext)
	return pluginCtx.Flush(data, int(length), C.GoString(tag))
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {}
