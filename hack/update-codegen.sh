#!/usr/bin/env bash

# Copyright 2017 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
CODEGEN_PKG=${CODEGEN_PKG:-$(cd "${SCRIPT_ROOT}"; ls -d -1 ./vendor/k8s.io/code-generator 2>/dev/null || echo ../code-generator)}
cd ${SCRIPT_ROOT}

echo "job_controller gen start"

bash "${CODEGEN_PKG}"/generate-groups.sh \
   "deepcopy,defaulter" \
  github.com/kube-queue/pytorch-operator-extension/pkg/pytorch-operator/client/common \
  github.com/kube-queue/pytorch-operator-extension/pkg/pytorch-operator/apis/common \
  "job_controller:v1" \
  --go-header-file "${SCRIPT_ROOT}"/hack/boilerplate/boilerplate.generatego.txt

echo "pytorch v1 gen start"

bash "${CODEGEN_PKG}"/generate-groups.sh \
  all \
  github.com/kube-queue/pytorch-operator-extension/pkg/pytorch-operator/client \
  github.com/kube-queue/pytorch-operator-extension/pkg/pytorch-operator/apis \
  "pytorch:v1" \
  --go-header-file "${SCRIPT_ROOT}"/hack/boilerplate.go.txt

echo "Generating defaulters for pytorch/v1"
${GOPATH}/bin/defaulter-gen  --input-dirs github.com/kube-queue/pytorch-operator-extension/pkg/pytorch-operator/apis/pytorch/v1 -O zz_generated.defaults --go-header-file "${SCRIPT_ROOT}"/hack/boilerplate/boilerplate.generatego.txt "$@"