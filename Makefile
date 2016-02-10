SRC_PATH = src
PROTOS_PATH = protos
LIB_PATH = lib/orchlib

CXX = g++
CPPFLAGS += -I/usr/local/include -pthread
CXXFLAGS += -std=c++11 -fPIC -I$(SRC_PATH) -O3
LDFLAGS += -L/usr/local/lib -lgrpc++_unsecure -lgrpc -lprotobuf -lpthread -ldl
PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

vpath %.proto $(PROTOS_PATH)

all: system-check $(LIB_PATH)/liborchlib.so $(SRC_PATH)/server lib/orchpy/orchpy/liborchlib.so

$(LIB_PATH)/liborchlib.so: $(SRC_PATH)/types.pb.o $(SRC_PATH)/orchestra.pb.o $(SRC_PATH)/types.grpc.pb.o $(SRC_PATH)/orchestra.grpc.pb.o $(LIB_PATH)/orchlib.o
	$(CXX) $^ $(LDFLAGS) -shared -o $@

$(SRC_PATH)/server: $(SRC_PATH)/orchestra.pb.o $(SRC_PATH)/types.pb.o $(SRC_PATH)/types.grpc.pb.o $(SRC_PATH)/orchestra.grpc.pb.o $(SRC_PATH)/server.o
	$(CXX) $^ $(LDFLAGS) -o $@

.PRECIOUS: ./src/%.grpc.pb.cc
$(SRC_PATH)/%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=$(SRC_PATH)/ --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

.PRECIOUS: ./src/%.pb.cc
$(SRC_PATH)/%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=./src $<

lib/orchpy/orchpy/liborchlib.so:
	cp -f lib/orchlib/liborchlib.so lib/orchpy/orchpy/liborchlib.so

clean:
	rm -f $(SRC_PATH)/*.o $(LIB_PATH)/*.o $(SRC_PATH)/*.pb.cc $(SRC_PATH)/*.pb.h $(LIB_PATH)/orchlib.so $(SRC_PATH)/server


# The following is to test your system and ensure a smoother experience.
# They are by no means necessary to actually compile a grpc-enabled software.

PROTOC_CMD = which $(PROTOC)
PROTOC_CHECK_CMD = $(PROTOC) --version | grep -q libprotoc.3
PLUGIN_CHECK_CMD = which $(GRPC_CPP_PLUGIN)
HAS_PROTOC = $(shell $(PROTOC_CMD) > /dev/null && echo true || echo false)
ifeq ($(HAS_PROTOC),true)
HAS_VALID_PROTOC = $(shell $(PROTOC_CHECK_CMD) 2> /dev/null && echo true || echo false)
endif
HAS_PLUGIN = $(shell $(PLUGIN_CHECK_CMD) > /dev/null && echo true || echo false)

SYSTEM_OK = false
ifeq ($(HAS_VALID_PROTOC),true)
ifeq ($(HAS_PLUGIN),true)
SYSTEM_OK = true
endif
endif

system-check:
ifneq ($(HAS_VALID_PROTOC),true)
	@echo " DEPENDENCY ERROR"
	@echo
	@echo "You don't have protoc 3.0.0 installed in your path."
	@echo "Please install Google protocol buffers 3.0.0 and its compiler."
	@echo "You can find it here:"
	@echo
	@echo "   https://github.com/google/protobuf/releases/tag/v3.0.0-alpha-1"
	@echo
	@echo "Here is what I get when trying to evaluate your version of protoc:"
	@echo
	-$(PROTOC) --version
	@echo
	@echo
endif
ifneq ($(HAS_PLUGIN),true)
	@echo " DEPENDENCY ERROR"
	@echo
	@echo "You don't have the grpc c++ protobuf plugin installed in your path."
	@echo "Please install grpc. You can find it here:"
	@echo
	@echo "   https://github.com/grpc/grpc"
	@echo
	@echo "Here is what I get when trying to detect if you have the plugin:"
	@echo
	-which $(GRPC_CPP_PLUGIN)
	@echo
	@echo
endif
ifneq ($(SYSTEM_OK),true)
	@false
endif
