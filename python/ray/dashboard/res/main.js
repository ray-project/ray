let dashboard = new Vue({
    el: "#dashboard",
    data: {
        now: (new Date()).getTime() / 1000,
        shown: {},
        error: "loading...",
        last_update: undefined,
        clients: undefined,
        totals: undefined,
        ray_config: undefined,
    },
    methods: {
        updateNodeInfo() {
            axios.get("/api/node_info").then(function (resp) {
                setTimeout(dashboard.updateNodeInfo, 500);

                dashboard.error = resp.data.error;
                dashboard.last_update = resp.data.timestamp;
                if (resp.data.error) {
                    dashboard.clients = undefined;
                    dashboard.tasks = undefined;
                    dashboard.totals = undefined;
                    return;
                }
                dashboard.clients = resp.data.result.clients;
                dashboard.tasks = resp.data.result.tasks;
                dashboard.totals = resp.data.result.totals;
            }).catch(function() {
                setTimeout(dashboard.updateNodeInfo, 500);

                dashboard.error = "request error"
                dashboard.clients = undefined;
                dashboard.tasks = undefined;
                dashboard.totals = undefined;
            });
        },
        updateRayConfig() {
            axios.get("/api/ray_config").then(function (resp) {
                setTimeout(dashboard.updateRayConfig, 10000);

                if (resp.data.error) {
                    dashboard.ray_config = undefined;
                    return;
                }
                dashboard.ray_config = resp.data.result;
            }).catch(function() {
                setTimeout(dashboard.updateRayConfig, 10000);

                dashboard.error = "request error"
                dashboard.ray_config = undefined;
            });
        },
        updateAll() {
            this.updateNodeInfo();
            this.updateRayConfig();
        },
        tickClock() {
            this.now = (new Date()).getTime() / 1000;
        }
    },
    filters: {
        age(ts) {
            return (dashboard.now - ts | 0) + "s";
        },
        si(x) {
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
            {{workers}}/{{cores}} ({{(frac*100).toFixed(0)}}%)
        </td>
    `,
});

Vue.component("node", {
    props: ['v', 'ip', 'now'],
    data: function() {
        return {
            hidden: true,
        };
    },
    computed: {
        n_workers: function() {
            if (this.v.workers) { return this.v.workers.length; }
            return 0;
        },
        age: function() {
            if (this.v.boot_time) {
                let rs = this.now - this.v.boot_time | 0;
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
    },
    template: `
    <tbody v-on:click="toggleHide()">
        <tr class="ray_node">
            <td class="ip">{{ip}}</td>
            <td class="uptime">{{age}}</td>
            <worker-usage
                :cores="v.cpus[0]"
                :workers="n_workers"
            ></worker-usage>
            <usagebar v-if="v.mem"
                :avail="v.mem[1]" :total="v.mem[0]"
                stat="mem"
            ></usagebar>
            <usagebar v-if="v.disk"
                :avail="v.disk['/'].free" :total="v.disk['/'].total"
                stat="storage"
            ></usagebar>
            <loadbar
                v-if="v.load_avg"
                :cores="v.cpus[0]"
                :onem="v.load_avg[0][0]"
                :fivem="v.load_avg[0][1]"
                :fifteenm="v.load_avg[0][2]"
            >
            </loadbar>
        </tr>
        <template v-if="!hidden && v.workers">
            <tr class="workerlist">
                <th>time</th>
                <th>name</th>
                <th>pid</th>
                <th>uss</th>
            </tr>
            <tr class="workerlist" v-for="x in v.workers">
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
        si(x) {
            let prefixes = ["B", "K", "M", "G", "T"]
            let i = 0;
            while (x > 1024) {
                x /= 1024;
                i += 1;
            }
            return `${x.toFixed(1)}${prefixes[i]}`;
        },
        pct(x) {
            return `${(x*100).toFixed(0)}%`;
        },
    },
    template: `
    <td class="usagebar" :class="tcls">
        {{used | si}}/{{total | si}} ({{ frac | pct }})
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
        {{onem}} ({{frac.toFixed(2)}}), {{fivem}}, {{fifteenm}}
    </td>
    `,
});

setInterval(dashboard.tickClock, 1000);
dashboard.updateAll();
