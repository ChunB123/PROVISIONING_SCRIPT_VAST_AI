#!/bin/bash

source /venv/main/bin/activate
COMFYUI_DIR=${WORKSPACE}/ComfyUI

# Packages are installed after nodes so we can fix them...

APT_PACKAGES=(
    #"package-1"
    #"package-2"
)

PIP_PACKAGES=(
    "boto3"
    "requests"
)

NODES=(
    "https://github.com/ChunB123/ComfyUI-LTXVideo"
    "https://github.com/ChunB123/ComfyUI-Easy-Use"
    "https://github.com/ChunB123/ComfyUI-KJNodes"
    "https://github.com/ChunB123/ComfyUI-VideoHelperSuite"
    "https://github.com/ChunB123/ComfyUI_Comfyroll_CustomNodes"
    "https://github.com/ChunB123/comfyui-various"
    "https://github.com/ChunB123/ComfyUI-MelBandRoFormer"
    "https://github.com/ChunB123/CRT-Nodes"
    "https://github.com/ChunB123/ComfyUI-S3-IO"
)

WORKFLOWS=(

)

CHECKPOINT_MODELS=(
)

UNET_MODELS=(
)

DIFFUSION_MODELS=(
    "https://huggingface.co/MichaelXu123/LTXV2_comfy/resolve/main/diffusion_models/ltx-2-19b-dev_transformer_only_bf16.safetensors"
    "https://huggingface.co/MichaelXu123/MelBandRoFormer_comfy/resolve/main/MelBandRoformer_fp16.safetensors"
)

LORA_MODELS=(
    "https://huggingface.co/MichaelXu123/LTX-2/resolve/main/ltx-2-19b-distilled-lora-384.safetensors"
    "https://huggingface.co/MichaelXu123/LTX-2-19b-IC-LoRA-Detailer/resolve/main/ltx-2-19b-ic-lora-detailer.safetensors"
    "https://huggingface.co/MichaelXu123/LTX2_Herocam_Lora/resolve/main/HeroCam_LTX2_bucket113_step_1500.safetensors"
)

VAE_MODELS=(
    "https://huggingface.co/MichaelXu123/LTXV2_comfy/resolve/main/VAE/LTX2_video_vae_bf16.safetensors"
    "https://huggingface.co/MichaelXu123/LTXV2_comfy/resolve/main/VAE/LTX2_audio_vae_bf16.safetensors"
)

TEXT_ENCODER_MODELS=(
    "https://huggingface.co/MichaelXu123/comfy-ltx-2/resolve/main/split_files/text_encoders/gemma_3_12B_it_fp8_scaled.safetensors"
)

CLIP_MODELS=(
    "https://huggingface.co/MichaelXu123/LTXV2_comfy/resolve/main/text_encoders/ltx-2-19b-embeddings_connector_distill_bf16.safetensors"
)

ESRGAN_MODELS=(
)

CONTROLNET_MODELS=(
)

### DO NOT EDIT BELOW HERE UNLESS YOU KNOW WHAT YOU ARE DOING ###

function provisioning_start() {
    provisioning_print_header
    provisioning_get_apt_packages
    provisioning_get_nodes
    provisioning_get_pip_packages
    provisioning_get_files \
        "${COMFYUI_DIR}/models/checkpoints" \
        "${CHECKPOINT_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/unet" \
        "${UNET_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/diffusion_models" \
        "${DIFFUSION_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/loras" \
        "${LORA_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/controlnet" \
        "${CONTROLNET_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/vae" \
        "${VAE_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/text_encoders" \
        "${TEXT_ENCODER_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/clip" \
        "${CLIP_MODELS[@]}"
    provisioning_get_files \
        "${COMFYUI_DIR}/models/esrgan" \
        "${ESRGAN_MODELS[@]}"
    # Download and install the SQS consumer with supervisord
    CONSUMER_URL="https://raw.githubusercontent.com/ChunB123/PROVISIONING_SCRIPT_VAST_AI/refs/heads/main/consumer.py"
    SUPERVISOR_CONF_URL="https://raw.githubusercontent.com/ChunB123/PROVISIONING_SCRIPT_VAST_AI/refs/heads/main/consumer-supervisor.conf"
    CONSUMER_PATH="/workspace/consumer.py"
    printf "Downloading consumer.py...\n"
    wget -qO "$CONSUMER_PATH" "$CONSUMER_URL"
    if [[ -f "$CONSUMER_PATH" ]]; then
        printf "Setting up consumer with supervisord...\n"
        # Install supervisor config
        mkdir -p /etc/supervisor/conf.d
        wget -qO /etc/supervisor/conf.d/consumer.conf "$SUPERVISOR_CONF_URL"
        # Start supervisord now and on reboot
        /usr/bin/supervisord -c /etc/supervisor/conf.d/consumer.conf &
        ( crontab -l 2>/dev/null; echo "@reboot /usr/bin/supervisord -c /etc/supervisor/conf.d/consumer.conf" ) | crontab -
        printf "Consumer service started via supervisord.\n"
    else
        printf "WARNING: Failed to download consumer.py\n"
    fi
    provisioning_print_end
}

function provisioning_get_apt_packages() {
    if [[ -n $APT_PACKAGES ]]; then
            sudo $APT_INSTALL ${APT_PACKAGES[@]}
    fi
}

function provisioning_get_pip_packages() {
    if [[ -n $PIP_PACKAGES ]]; then
            pip install --no-cache-dir ${PIP_PACKAGES[@]}
    fi
}

function provisioning_get_nodes() {
    for repo in "${NODES[@]}"; do
        dir="${repo##*/}"
        path="${COMFYUI_DIR}/custom_nodes/${dir}"
        requirements="${path}/requirements.txt"
        if [[ -d $path ]]; then
            if [[ ${AUTO_UPDATE,,} != "false" ]]; then
                printf "Updating node: %s...\n" "${repo}"
                ( cd "$path" && git pull )
                if [[ -e $requirements ]]; then
                   pip install --no-cache-dir -r "$requirements"
                fi
            fi
        else
            printf "Downloading node: %s...\n" "${repo}"
            git clone "${repo}" "${path}" --recursive
            if [[ -e $requirements ]]; then
                pip install --no-cache-dir -r "${requirements}"
            fi
            if [[ -e "${path}/install.py" ]]; then
                printf "Running install.py for %s...\n" "${dir}"
                ( cd "${path}" && python install.py )
            fi
        fi
    done
}

function provisioning_get_files() {
    if [[ -z $2 ]]; then return 1; fi
    
    dir="$1"
    mkdir -p "$dir"
    shift
    arr=("$@")
    printf "Downloading %s model(s) to %s...\n" "${#arr[@]}" "$dir"
    for url in "${arr[@]}"; do
        printf "Downloading: %s\n" "${url}"
        provisioning_download "${url}" "${dir}"
        printf "\n"
    done
}

function provisioning_print_header() {
    printf "\n##############################################\n#                                            #\n#          Provisioning container            #\n#                                            #\n#         This will take some time           #\n#                                            #\n# Your container will be ready on completion #\n#                                            #\n##############################################\n\n"
}

function provisioning_print_end() {
    printf "\nProvisioning complete:  Application will start now\n\n"
}

function provisioning_has_valid_hf_token() {
    [[ -n "$HF_TOKEN" ]] || return 1
    url="https://huggingface.co/api/whoami-v2"

    response=$(curl -o /dev/null -s -w "%{http_code}" -X GET "$url" \
        -H "Authorization: Bearer $HF_TOKEN" \
        -H "Content-Type: application/json")

    # Check if the token is valid
    if [ "$response" -eq 200 ]; then
        return 0
    else
        return 1
    fi
}

function provisioning_has_valid_civitai_token() {
    [[ -n "$CIVITAI_TOKEN" ]] || return 1
    url="https://civitai.com/api/v1/models?hidden=1&limit=1"

    response=$(curl -o /dev/null -s -w "%{http_code}" -X GET "$url" \
        -H "Authorization: Bearer $CIVITAI_TOKEN" \
        -H "Content-Type: application/json")

    # Check if the token is valid
    if [ "$response" -eq 200 ]; then
        return 0
    else
        return 1
    fi
}

# Download from $1 URL to $2 file path
function provisioning_download() {
    if [[ -n $HF_TOKEN && $1 =~ ^https://([a-zA-Z0-9_-]+\.)?huggingface\.co(/|$|\?) ]]; then
        auth_token="$HF_TOKEN"
    elif 
        [[ -n $CIVITAI_TOKEN && $1 =~ ^https://([a-zA-Z0-9_-]+\.)?civitai\.com(/|$|\?) ]]; then
        auth_token="$CIVITAI_TOKEN"
    fi
    if [[ -n $auth_token ]];then
        wget --header="Authorization: Bearer $auth_token" -qnc --content-disposition --show-progress -e dotbytes="${3:-4M}" -P "$2" "$1"
    else
        wget -qnc --content-disposition --show-progress -e dotbytes="${3:-4M}" -P "$2" "$1"
    fi
}

# Allow user to disable provisioning if they started with a script they didn't want
if [[ ! -f /.noprovisioning ]]; then
    provisioning_start
fi
