let dashboard = new Vue({
    el: "#dashboard",
    data: {
        now: (new Date()).getTime() / 1000,
        shown: {},
        error: "loading...",
        last_update: undefined,
        clients: undefined,
        totals: undefined,
        tasks: undefined,
        ray_config: undefined,
    },
    methods: {
        updateNodeInfo: function() {
            var self = this;
            fetch("/api/node_info").then(function (resp) {
                return resp.json();
            }).then(function(data) {
                self.error = data.error;
                if (data.error) {
                    self.clients = undefined;
                    self.tasks = undefined;
                    self.totals = undefined;
                    return;
                }
                self.last_update = data.timestamp;
                self.clients = data.result.clients;
                self.tasks = data.result.tasks;
                self.totals = data.result.totals;
            }).catch(function() {
                self.error = "request error"
                self.clients = undefined;
                self.tasks = undefined;
                self.totals = undefined;
            }).finally(function() {
                setTimeout(self.updateNodeInfo, 500);
            });
        },
        updateRayConfig: function() {
            var self = this;
            fetch("/api/ray_config").then(function (resp) {
                return resp.json();
            }).then(function(data) {
                if (data.error) {
                    self.ray_config = undefined;
                    return;
                }
                self.ray_config = data.result;
            }).catch(function() {
                self.error = "request error"
                self.ray_config = undefined;
            }).finally(function() {
                setTimeout(self.updateRayConfig, 10000);
            });
        },
        updateAll: function() {
            this.updateNodeInfo();
            this.updateRayConfig();
        },
        tickClock: function() {
            this.now = (new Date()).getTime() / 1000;
        }
    },
    computed: {
        outdated_cls: function(ts) {
            if ((this.now - this.last_update) > 5) {
                return "outdated";
            }
            return "";
        },
        age: function(ts) {
            return (this.now - this.last_update | 0) + "s";
        },
    },
    filters: {
        si: function(x) {
            let prefixes = ["B", "K", "M", "G", "T"]
            let i = 0;
            while (x > 1024) {
                x /= 1024;
                i += 1;
            }
            return `${x.toFixed(1)}${prefixes[i]}`;
        },
    },
});

Vue.component("worker-usage", {
    props: ['cores', 'workers'],
    computed: {
        frac: function() {
            return this.workers / this.cores;
        },
        cls: function() {
            if (this.frac > 3) { return "critical"; }
            if (this.frac > 2) { return "bad"; }
            if (this.frac > 1.5) { return "high"; }
            if (this.frac > 1) { return "average"; }
            return "low";
        },
    },
    template: `
        <td class="workers" :class="cls">
            {{workers}}/{{cores}} {{(frac*100).toFixed(0)}}%
        </td>
    `,
});

Vue.component("node", {
    props: [
        "now",
        "hostname",
        "boot_time",
        "n_workers",
        "n_cores",
        "m_avail",
        "m_total",
        "d_avail",
        "d_total",
        "load",
        "n_sent",
        "n_recv",
        "workers",
    ],
    data: function() {
        return {
            hidden: true,
        };
    },
    computed: {
        age: function() {
            if (this.boot_time) {
                let n = this.now;
                if (this.boot_time > 2840140800) {
                    // Hack. It's a sum of multiple nodes.
                    n *= this.hostname;
                }
                let rs = n - this.boot_time | 0;
                let s = rs % 60;
                let m = ((rs / 60) % 60) | 0;
                let h = (rs / 3600) | 0;
                if (h) {
                    return `${h}h ${m}m ${s}s`;
                }
                if (m) {
                    return `${m}m ${s}s`;
                }
                return `${s}s`;
            }
            return "?"
        },
    },
    methods: {
        toggleHide: function() {
            this.hidden = !this.hidden;
        }
    },
    filters: {
        mib(x) {
            return `${(x/(1024**2)).toFixed(3)}M`;
        },
        hostnamefilter(x) {
            if (isNaN(x)) {
                return x;
            }
            return `Totals: ${x} nodes`;
        },
    },
    template: `
    <tbody v-on:click="toggleHide()">
        <tr class="ray_node">
            <td class="hostname">{{hostname | hostnamefilter}}</td>
            <td class="uptime">{{age}}</td>
            <worker-usage
                :workers="n_workers"
                :cores="n_cores"
            ></worker-usage>
            <usagebar
                :avail="m_avail" :total="m_total"
                stat="mem"
            ></usagebar>
            <usagebar
                :avail="d_avail" :total="d_total"
                stat="storage"
            ></usagebar>
            <loadbar
                :cores="n_cores"
                :onem="load[0]"
                :fivem="load[1]"
                :fifteenm="load[2]"
            >
            </loadbar>
            <td class="netsent">{{n_sent | mib}}/s</td>
            <td class="netrecv">{{n_recv | mib}}/s</td>
        </tr>
        <template v-if="!hidden && workers">
            <tr class="workerlist">
                <th>time</th>
                <th>name</th>
                <th>pid</th>
                <th>uss</th>
            </tr>
            <tr class="workerlist" v-for="x in workers">
                <td>user: {{x.cpu_times.user}}s</td>
                <td>{{x.name}}</td>
                <td>{{x.pid}}</td>
                <td>{{(x.memory_full_info.uss/1048576).toFixed(0)}}MiB</td>
            </tr>
        </template>
    </tbody>
    `,
});

Vue.component("usagebar", {
    props: ['stat', 'avail', 'total'], // e.g. free -m avail
    computed: {
        used: function() { return this.total - this.avail; },
        frac: function() { return (this.total - this.avail)/this.total; },
        cls: function() {
            if (this.frac > 0.95) { return "critical"; }
            if (this.frac > 0.9) { return "bad"; }
            if (this.frac > 0.8) { return "high"; }
            if (this.frac > 0.5) { return "average"; }
            return "low";
        },
        tcls: function() {
            return `${this.stat} ${this.cls}`;
        }
    },
    filters: {
        gib(x) {
            return `${(x/(1024**3)).toFixed(1)}G`;
        },
        pct(x) {
            return `${(x*100).toFixed(0)}%`;
        },
    },
    template: `
    <td class="usagebar" :class="tcls">
        {{used | gib}}/{{total | gib}} {{ frac | pct }}
    </td>
    `,
});

Vue.component("loadbar", {
    props: ['cores', 'onem', 'fivem', 'fifteenm'],
    computed: {
        frac: function() { return this.onem/this.cores; },
        cls: function() {
            if (this.frac > 3) { return "critical"; }
            if (this.frac > 2.5) { return "bad"; }
            if (this.frac > 2) { return "high"; }
            if (this.frac > 1.5) { return "average"; }
            return "low";
        },
    },
    template: `
    <td class="load loadbar" :class="cls">
        {{onem.toFixed(2)}}, {{fivem.toFixed(2)}}, {{fifteenm.toFixed(2)}}
    </td>
    `,
});

setInterval(dashboard.tickClock, 1000);
dashboard.updateAll();
