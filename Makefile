gen-api: gen-curp-api gen-xline-api

gen-xline-api:
	rm -rf api/xline && \
	mkdir -p api/xline && \
	cd xline-proto/src && \
	protoc \
		--experimental_allow_proto3_optional \
		--go_out=../../api/xline \
		--go_opt=paths=source_relative \
		--go-grpc_out=../../api/xline \
		--go-grpc_opt=paths=source_relative \
	*.proto

gen-curp-api:
	rm -rf api/curp && \
	mkdir -p api/curp && \
	cd curp-proto/src && \
	protoc \
		--experimental_allow_proto3_optional \
		--go_out=../../api/curp \
		--go_opt=paths=source_relative \
		--go-grpc_out=../../api/curp \
		--go-grpc_opt=paths=source_relative \
	*.proto
