#!/usr/bin/env bash

export KEYRINGCFG=$HOME/.local/share/python_keyring/keyringrc.cfg
## KEYRING

if [ ! -f $KEYRINGCFG ]; then
    echo "File not found!"
    mkdir -p  $HOME/.local/share/python_keyring/ && echo -e "[backend]\ndefault-keyring=keyrings.alt.file.PlaintextKeyring" > $KEYRINGCFG
fi

## Disable promnt `Please enter password for encrypted keyring`
#RUN sudo -E echo "$keyring_pass" | python3 -m keyring set "REDIS" "ektovav"
printf "$keyring_pass" | python3 -c "import keyring; keyring.set_password(\"REDIS\", \"ektovav\", \"$1\")"
