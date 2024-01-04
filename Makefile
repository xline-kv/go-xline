gen-api: gen-curp-api gen-xline-api

gen-curp-api:
	rm -rf api/gen/curp && \
	mkdir -p api/gen/curp && \
	cd api/proto/curp-proto/src && \
	protoc \
		--experimental_allow_proto3_optional \
		--go_out=../../../gen/curp \
		--go_opt=paths=source_relative \
		--go-grpc_out=../../../gen/curp \
		--go-grpc_opt=paths=source_relative \
	*.proto

gen-xline-api:
	rm -rf api/gen/xline && \
	mkdir -p api/gen/xline && \
	cd api/proto/xline-proto/src && \
	protoc \
		--experimental_allow_proto3_optional \
		--go_out=../../../gen/xline \
		--go_opt=paths=source_relative \
		--go-grpc_out=../../../gen/xline \
		--go-grpc_opt=paths=source_relative \
	*.proto
