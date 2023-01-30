#test for tailscale up
tailscale status -json | jq -r .BackendState | grep -q "Running" || exit 1

#test for crate connectivity
clusterhosts=$(curl -s -u "${TSAPIKEY}:" https://api.tailscale.com/api/v2/tailnet/jcoffi.github/devices | jq -r '.devices[].name')
#clusterhosts=$(echo $response | tr ' ' '\n' | awk '{print $0".chimp-beta.ts.net"}' | tr '\n' ' ')
if [ -n clusterhosts ]; then
    clusterhosts=${clusterhosts%?}
else
    clusterhosts="nexus.chimp-beta.ts.net"
fi

/usr/local/bin/crash -U crate --hosts $clusterhosts -c "SELECT * FROM sys.nodes" || exit 1

ray list nodes -f NODE_NAME=nexus.chimp-beta.ts.net -f STATE=ALIVE || exit 1
#ray status || exit 1

#exit 0