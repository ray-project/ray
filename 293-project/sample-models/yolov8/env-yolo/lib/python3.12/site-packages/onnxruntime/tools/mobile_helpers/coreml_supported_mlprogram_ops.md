<!--
Keep in sync with doco generated from /docs/execution-providers/CoreML-ExecutionProvider.md on the gh_pages branch
-->
|Operator|Note|
|--------|------|
|ai.onnx:Add||
|ai.onnx:AveragePool|Only 2D Pool is supported currently. 3D and 5D support can be added if needed.|
|ai.onnx:Clip||
|ai.onnx:Concat||
|ai.onnx:Conv|Only 1D/2D Conv is supported.<br/>Bias if provided must be constant.|
|ai.onnx:ConvTranspose|Weight and bias must be constant.<br/>padding_type of SAME_UPPER/SAME_LOWER is not supported.<br/>kernel_shape must have default values.<br/>output_shape is not supported.<br/>output_padding must have default values.|
|ai.onnx.DepthToSpace|If 'mode' is 'CRD' the input must have a fixed shape.|
|ai.onnx:Div||
|ai.onnx:Gemm|Input B must be constant.|
|ai.onnx:GlobalAveragePool|Only 2D Pool is supported currently. 3D and 5D support can be added if needed.|
|ai.onnx:GlobalMaxPool|Only 2D Pool is supported currently. 3D and 5D support can be added if needed.|
|ai.onnx:GridSample|4D input.<br/>'mode' of 'linear' or 'zeros'.<br/>(mode==linear && padding_mode==reflection && align_corners==0) is not supported.|
|ai.onnx.LeakyRelu||
|ai.onnx:MatMul|Only support for transA == 0, alpha == 1.0 and beta == 1.0 is currently implemented.|
|ai.onnx:MaxPool|Only 2D Pool is supported currently. 3D and 5D support can be added if needed.|
|ai.onnx:Mul||
|ai.onnx:Pow|Only supports cases when both inputs are fp32.|
|ai.onnx:Relu||
|ai.onnx:Reshape||
|ai.onnx:Resize|See [resize_op_builder.cc](https://github.com/microsoft/onnxruntime/blob/main/onnxruntime/core/providers/coreml/builders/impl/resize_op_builder.cc) implementation. There are too many permutations to describe the valid combinations.|
|ai.onnx.Slice|starts/ends/axes/steps must be constant initializers.|
|ai.onnx:Split||
|ai.onnx:Sub||
|ai.onnx:Sigmoid||
|ai:onnx:Tanh||
|ai.onnx:Transpose||
