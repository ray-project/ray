#test for tailscale up
tailscale status -json | jq -r .BackendState | grep -q "Running" || exit 1

#test for crate connectivity
response=$(curl -s -u "${TSAPIKEY}:" https://api.tailscale.com/api/v2/tailnet/jcoffi.github/devices | jq -r '.devices[].hostname')
hostnames=$(echo $response | tr ' ' '\n' | awk '{print $0".chimp-beta.ts.net"}' | tr '\n' ' ')
if [ -n hostnames ]; then
    hostnames=${hostnames%?}
else
    hostnames="nexus.chimp-beta.ts.net"
fi

/usr/local/bin/crash -U crate --hosts $hostnames -c "SELECT * FROM sys.nodes" || exit 1