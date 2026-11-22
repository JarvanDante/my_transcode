#!/usr/bin/env bash
# 把 my_transcode 发布到 k8s 测试环境：构建、打镜像、推送、改镜像、等待滚动。
# 用法: ./deploy_test.sh [--dry-run]，镜像仓库请提前自行 docker login。

# ======== 按实际环境修改以下变量 ========
# 服务名，仅用于日志。
SERVICE_NAME="my_transcode"
# 分组，供 my/deploy_test_all.sh 按 backend/frontend 筛选。
SERVICE_TYPE="backend"
# 镜像名，不含仓库前缀和 tag。
IMAGE_NAME="my_transcode"
# 镜像仓库前缀，为空则只构建本地镜像并跳过 push。
# docker-desktop 直接用本机镜像（如 my_transcode:latest），留空即可。
REGISTRY=""
# Dockerfile 路径，相对仓库根目录。
DOCKERFILE="Dockerfile"
# 测试环境命名空间。
NAMESPACE="my-test"
# Deployment 名。
DEPLOYMENT="my-transcode"
# Deployment 里要替换镜像的容器名。
CONTAINER="my-transcode"
# 允许发布的 kube context，空格分隔，为空时拒绝真实发布。
ALLOWED_CONTEXTS="docker-desktop"
# 允许发布的命名空间，空格分隔，为空时拒绝真实发布。
ALLOWED_NAMESPACES="my-test"
# 构建平台，Apple Silicon 发往 amd64 集群用 linux/amd64。
PLATFORM="linux/arm64"
# rollout status 等待超时。
ROLLOUT_TIMEOUT="180s"
# 前端构建命令，后端留空。
BUILD_CMD=""
# 构建后必须存在的产物，留空不检查。
BUILD_ARTIFACT=""

# ======== 以下为通用流程，一般不用改 ========
set -eo pipefail
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DRY_RUN=0
# 禁止发布的命名空间特征，防止误发生产。
PROD_PATTERN='(^|[-_])prod([-_]|$)|production'

die() { echo "错误: $*" >&2; exit 1; }
info() { echo "==> [$SERVICE_NAME] $*"; }
warn() { echo "警告: [$SERVICE_NAME] $*" >&2; }

# 按 dry-run 决定打印或执行命令。
run() {
  echo "+ $*"
  if [ "$DRY_RUN" -eq 0 ]; then "$@"; fi
}

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --platform) shift; PLATFORM="${1:-}" ;;
    -h|--help) echo "用法: ./deploy_test.sh [--dry-run] [--platform linux/amd64]"; exit 0 ;;
    *) die "未知参数 $1" ;;
  esac
  shift
done

# 任一关键变量含 TODO 则跳过，返回码 3 表示跳过。
for v in IMAGE_NAME DOCKERFILE NAMESPACE DEPLOYMENT CONTAINER; do
  case "${!v}" in *TODO*) warn "跳过：$v=${!v}"; exit 3 ;; esac
done
[ -f "$REPO_DIR/$DOCKERFILE" ] || die "找不到 $REPO_DIR/$DOCKERFILE"

in_list() {
  local item="$1"; shift
  local x
  for x in "$@"; do [ "$x" = "$item" ] && return 0; done
  return 1
}

# 校验 context 与 namespace，真实发布不通过则退出。
command -v kubectl >/dev/null 2>&1 || die "未安装 kubectl"
KUBE_CONTEXT="$(kubectl config current-context 2>/dev/null || true)"
GUARD_OK=1
if echo "$NAMESPACE" | grep -Eiq "$PROD_PATTERN"; then die "namespace '$NAMESPACE' 疑似生产，拒绝"; fi
# shellcheck disable=SC2086
if [ -z "$ALLOWED_CONTEXTS" ] || ! in_list "$KUBE_CONTEXT" $ALLOWED_CONTEXTS; then
  GUARD_OK=0; warn "当前 context '${KUBE_CONTEXT:-<无>}' 不在允许名单: ${ALLOWED_CONTEXTS:-<未配置>}"
fi
# shellcheck disable=SC2086
if [ -z "$ALLOWED_NAMESPACES" ] || ! in_list "$NAMESPACE" $ALLOWED_NAMESPACES; then
  GUARD_OK=0; warn "namespace '$NAMESPACE' 不在允许名单: ${ALLOWED_NAMESPACES:-<未配置>}"
fi
if [ "$GUARD_OK" -eq 0 ]; then
  if [ "$DRY_RUN" -eq 1 ]; then warn "dry-run 继续打印；真实执行会被拒绝"; else die "拒绝发布：测试环境校验未通过"; fi
fi
[ "$DRY_RUN" -eq 1 ] || command -v docker >/dev/null 2>&1 || die "未安装 docker"

# tag 用短 SHA，有未提交改动时加 -dirty 和时间避免同名不滚动。
TAG="$(git -C "$REPO_DIR" rev-parse --short HEAD)"
if [ -n "$(git -C "$REPO_DIR" status --porcelain)" ]; then TAG="${TAG}-dirty-$(date +%Y%m%d%H%M%S)"; fi
IMAGE="${REGISTRY:+${REGISTRY%/}/}${IMAGE_NAME}:${TAG}"
info "context=${KUBE_CONTEXT:-<无>} ns=$NAMESPACE deploy=$DEPLOYMENT image=$IMAGE platform=$PLATFORM dry-run=$DRY_RUN"

if [ -n "$BUILD_CMD" ]; then
  echo "+ (cd $REPO_DIR && $BUILD_CMD)"
  if [ "$DRY_RUN" -eq 0 ]; then (cd "$REPO_DIR" && bash -c "$BUILD_CMD"); fi
  if [ -n "$BUILD_ARTIFACT" ] && [ "$DRY_RUN" -eq 0 ] && [ ! -e "$REPO_DIR/$BUILD_ARTIFACT" ]; then
    die "构建后缺少 $BUILD_ARTIFACT"
  fi
fi

run docker buildx build --platform "$PLATFORM" -f "$REPO_DIR/$DOCKERFILE" -t "$IMAGE" --load "$REPO_DIR"
if [ -n "$REGISTRY" ]; then
  run docker push "$IMAGE"
else
  info "未配置 REGISTRY，跳过 push（仅适用于集群直接使用本机镜像）"
fi

run kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" set image "deployment/$DEPLOYMENT" "$CONTAINER=$IMAGE"
if ! run kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" rollout status "deployment/$DEPLOYMENT" --timeout="$ROLLOUT_TIMEOUT"; then
  warn "滚动未在 $ROLLOUT_TIMEOUT 内完成，可回滚："
  echo "    kubectl --context $KUBE_CONTEXT -n $NAMESPACE rollout undo deployment/$DEPLOYMENT" >&2
  exit 1
fi
if [ "$DRY_RUN" -eq 1 ]; then info "dry-run 结束，未执行任何命令"; else info "发布完成"; fi
