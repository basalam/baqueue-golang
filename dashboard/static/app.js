document.addEventListener("alpine:init", () => {
  Alpine.data("dashboard", () => ({
    tab: "overview",
    theme: localStorage.getItem("bq-theme") || "dark",
    connected: false,
    ws: null,
    jobsWS: null,
    jobsWSConnected: false,
    sidebarCollapsed: false,
    sidebarMobileOpen: false,

    totals: { pending: 0, processing: 0, completed: 0, failed: 0, queues: 0, total: 0 },
    rates: { pending_per_min: 0, processing_per_min: 0, completed_per_min: 0, failed_per_min: 0 },
    queues: [],
    supervisors: [],
    recentJobs: [],

    jobs: [],
    jobsPage: 1,
    jobsTotal: 0,
    jobsFilter: { queue: "", status: "", tag: "" },
    selectedJob: null,

    dateFrom: "",
    dateTo: "",

    refreshTimer: null,
    durationTimer: null,
    liveNow: Date.now(),

    get pageTitle() {
      const titles = { overview: "Overview", jobs: "Jobs", queues: "Queues", workers: "Workers" };
      return titles[this.tab] || "Dashboard";
    },

    init() {
      document.documentElement.setAttribute("data-theme", this.theme);
      this.fetchOverview();
      this.fetchSupervisors();
      this.fetchRecentJobs();
      this.connectWS();
      this.durationTimer = setInterval(() => {
        if (
          this.tab === "jobs" &&
          this.jobs.some((job) => this.inFlightAttempt(job))
        ) {
          const now = Date.now();
          if (Math.floor(now / 10) !== Math.floor(this.liveNow / 10)) {
            this.liveNow = now;
          }
        }
      }, 10);
      this.refreshTimer = setInterval(() => {
        if (this.tab === "overview") {
          this.fetchOverview();
          this.fetchRecentJobs();
        }
        if (this.tab === "jobs" && !this.jobsWSConnected) this.fetchJobs();
        if (this.tab === "workers") this.fetchSupervisors();
      }, 3000);
    },

    destroy() {
      if (this.refreshTimer) clearInterval(this.refreshTimer);
      if (this.durationTimer) clearInterval(this.durationTimer);
      if (this.ws) this.ws.close();
      this.disconnectJobsWS();
    },

    toggleTheme() {
      this.theme = this.theme === "dark" ? "light" : "dark";
      localStorage.setItem("bq-theme", this.theme);
      document.documentElement.setAttribute("data-theme", this.theme);
    },

    switchTab(t) {
      const prev = this.tab;
      this.tab = t;
      this.sidebarMobileOpen = false;
      if (t === "jobs") {
        this.fetchJobs();
        this.connectJobsWS();
      } else if (prev === "jobs") {
        this.disconnectJobsWS();
      }
      if (t === "overview") {
        this.fetchOverview();
        this.fetchRecentJobs();
      } else if (t === "workers") {
        this.fetchSupervisors();
      }
    },

    // ── Date Filters ────────────────────────────────────────

    getDateParams() {
      const params = {};
      if (this.dateFrom) {
        params.created_from = new Date(this.dateFrom).getTime() / 1000;
      }
      if (this.dateTo) {
        params.created_to = new Date(this.dateTo).getTime() / 1000;
      }
      return params;
    },

    onDateFilterChange() {
      this.fetchOverview();
      this.fetchRecentJobs();
      if (this.tab === "jobs") {
        this.jobsPage = 1;
        this.fetchJobs();
      }
    },

    clearDateFilter() {
      this.dateFrom = "";
      this.dateTo = "";
      this.onDateFilterChange();
    },

    setPreset(preset) {
      const now = new Date();
      let from = new Date();

      if (preset === "1h") from.setHours(now.getHours() - 1);
      else if (preset === "24h") from.setDate(now.getDate() - 1);
      else if (preset === "7d") from.setDate(now.getDate() - 7);
      else if (preset === "30d") from.setDate(now.getDate() - 30);

      this.dateFrom = this._toLocalISOString(from);
      this.dateTo = this._toLocalISOString(now);
      this.onDateFilterChange();
    },

    _toLocalISOString(d) {
      const pad = (n) => String(n).padStart(2, "0");
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
    },

    // ── WebSocket ───────────────────────────────────────────

    connectWS() {
      const proto = location.protocol === "https:" ? "wss:" : "ws:";
      const url = new URL("./ws", document.baseURI);
      url.protocol = proto;
      try {
        this.ws = new WebSocket(url);
        this.ws.onopen = () => { this.connected = true; };
        this.ws.onclose = () => {
          this.connected = false;
          setTimeout(() => this.connectWS(), 3000);
        };
        this.ws.onmessage = (e) => {
          const data = JSON.parse(e.data);
          if (!this.dateFrom && !this.dateTo) {
            this.totals = data.totals || this.totals;
            this.queues = data.queues || this.queues;
          }
          if (data.rates) this.rates = data.rates;
          this.supervisors = data.supervisors || this.supervisors;
        };
      } catch (err) {
        setTimeout(() => this.connectWS(), 3000);
      }
    },

    // ── Jobs WebSocket ──────────────────────────────────────

    connectJobsWS() {
      if (this.jobsWS) return;
      const proto = location.protocol === "https:" ? "wss:" : "ws:";
      const url = new URL("./ws/jobs", document.baseURI);
      url.protocol = proto;
      try {
        const ws = new WebSocket(url);
        this.jobsWS = ws;
        ws.onopen = () => {
          this.jobsWSConnected = true;
          this._sendJobsSubscription();
        };
        ws.onmessage = (e) => {
          try {
            const data = JSON.parse(e.data);
            this._mergeJobs(data.jobs || []);
            this.jobsTotal = data.total || 0;
          } catch (_err) { /* ignore */ }
        };
        ws.onclose = () => {
          this.jobsWSConnected = false;
          this.jobsWS = null;
          // If user is still on the jobs tab, reconnect after a short delay.
          if (this.tab === "jobs") setTimeout(() => this.connectJobsWS(), 2000);
        };
      } catch (_err) { /* ignore */ }
    },

    disconnectJobsWS() {
      this.jobsWSConnected = false;
      if (this.jobsWS) {
        const ws = this.jobsWS;
        this.jobsWS = null;
        ws.onclose = null;
        try { ws.close(); } catch (_e) { /* ignore */ }
      }
    },

    _sendJobsSubscription() {
      if (!this.jobsWS || this.jobsWS.readyState !== WebSocket.OPEN) return;
      const dp = this.getDateParams();
      this.jobsWS.send(JSON.stringify({
        type: "subscribe",
        filters: {
          queue: this.jobsFilter.queue || null,
          status: this.jobsFilter.status || null,
          tag: this.jobsFilter.tag || null,
          page: this.jobsPage,
          per_page: 25,
          created_from: dp.created_from || null,
          created_to: dp.created_to || null,
        },
      }));
    },

    _mergeJobs(newList) {
      const oldMap = new Map((this.jobs || []).map((j) => [j.id, j]));
      const now = Date.now();
      this.jobs = newList.map((j) => {
        const old = oldMap.get(j.id);
        if (!old) return { ...j, _entered: now };
        if (
          old.status !== j.status ||
          old.attempts !== j.attempts ||
          old.updated_at !== j.updated_at
        ) {
          return { ...j, _flash: now };
        }
        return j;
      });
    },

    // ── API calls ───────────────────────────────────────────

    async fetchOverview() {
      try {
        const dp = this.getDateParams();
        if (dp.created_from || dp.created_to) {
          const params = new URLSearchParams();
          if (dp.created_from) params.set("created_from", dp.created_from);
          if (dp.created_to) params.set("created_to", dp.created_to);
          const r = await fetch(`./api/stats?${params}`);
          const data = await r.json();
          this.totals = {
            pending: data.statuses?.pending || 0,
            processing: data.statuses?.processing || 0,
            completed: data.statuses?.completed || 0,
            failed: data.statuses?.failed || 0,
            total: data.total || 0,
            queues: Object.keys(data.queues || {}).length,
          };
          this.queues = Object.entries(data.queues || {}).map(([name, s]) => ({
            name,
            pending: s.pending || 0,
            processing: s.processing || 0,
            completed: s.completed || 0,
            failed: s.failed || 0,
          }));
        } else {
          const r = await fetch("./api/overview");
          const data = await r.json();
          this.totals = data.totals || this.totals;
          this.queues = data.queues || this.queues;
          this.supervisors = data.supervisors || this.supervisors;
          if (data.rates) this.rates = data.rates;
        }
      } catch (e) { /* ignore */ }
    },

    async fetchRecentJobs() {
      try {
        const params = new URLSearchParams({ per_page: "10", page: "1" });
        const dp = this.getDateParams();
        if (dp.created_from) params.set("created_from", dp.created_from);
        if (dp.created_to) params.set("created_to", dp.created_to);
        const r = await fetch(`./api/jobs?${params}`);
        const data = await r.json();
        this.recentJobs = data.jobs || [];
      } catch (e) { /* ignore */ }
    },

    async fetchSupervisors() {
      try {
        const r = await fetch("./api/supervisors");
        const data = await r.json();
        this.supervisors = data.supervisors || [];
      } catch (e) { /* ignore */ }
    },

    async fetchJobs() {
      // When the live WS is connected, just resubscribe with current filters —
      // the server pushes the snapshot back on the same channel.
      if (this.jobsWSConnected) {
        this._sendJobsSubscription();
        return;
      }
      try {
        const params = new URLSearchParams();
        if (this.jobsFilter.queue) params.set("queue", this.jobsFilter.queue);
        if (this.jobsFilter.status) params.set("status", this.jobsFilter.status);
        if (this.jobsFilter.tag) params.set("tag", this.jobsFilter.tag);
        params.set("page", this.jobsPage);
        params.set("per_page", "25");

        const dp = this.getDateParams();
        if (dp.created_from) params.set("created_from", dp.created_from);
        if (dp.created_to) params.set("created_to", dp.created_to);

        const r = await fetch(`./api/jobs?${params}`);
        const data = await r.json();
        this._mergeJobs(data.jobs || []);
        this.jobsTotal = data.total || 0;
      } catch (e) { /* ignore */ }
    },

    async viewJob(jobId) {
      try {
        const r = await fetch(`./api/jobs/${jobId}`);
        this.selectedJob = await r.json();
      } catch (e) { /* ignore */ }
    },

    closeModal() { this.selectedJob = null; },

    async retryJob(jobId) {
      await fetch(`./api/jobs/${jobId}/retry`, { method: "POST" });
      this.closeModal();
      this.fetchJobs();
      this.fetchOverview();
    },

    async executeJob(jobId) {
      // Promote a scheduled/pending job so it runs immediately.
      await fetch(`./api/jobs/${jobId}/execute`, { method: "POST" });
      this.closeModal();
      this.fetchJobs();
      this.fetchOverview();
    },

    async retryAllFailed() {
      const parts = [];
      if (this.jobsFilter.queue) parts.push(`queue "${this.jobsFilter.queue}"`);
      if (this.jobsFilter.tag) parts.push(`tag "${this.jobsFilter.tag}"`);
      const scope = parts.length ? ` (${parts.join(", ")})` : "";
      if (!confirm(`Retry ${this.jobsTotal} failed job(s)${scope}?`)) return;
      try {
        const params = new URLSearchParams();
        if (this.jobsFilter.queue) params.set("queue", this.jobsFilter.queue);
        if (this.jobsFilter.tag) params.set("tag", this.jobsFilter.tag);
        const dp = this.getDateParams();
        if (dp.created_from) params.set("created_from", dp.created_from);
        if (dp.created_to) params.set("created_to", dp.created_to);
        await fetch(`./api/jobs/retry-failed?${params}`, { method: "POST" });
        this.fetchJobs();
        this.fetchOverview();
      } catch (_e) { /* ignore */ }
    },

    async deleteJob(jobId) {
      await fetch(`./api/jobs/${jobId}`, { method: "DELETE" });
      this.closeModal();
      this.fetchJobs();
      this.fetchOverview();
    },

    prevPage() {
      if (this.jobsPage > 1) { this.jobsPage--; this.fetchJobs(); }
    },

    nextPage() {
      const maxPage = Math.ceil(this.jobsTotal / 25);
      if (this.jobsPage < maxPage) { this.jobsPage++; this.fetchJobs(); }
    },

    // ── Helpers ─────────────────────────────────────────────

    queueTotal(q) {
      return (q.pending || 0) + (q.processing || 0) + (q.completed || 0) + (q.failed || 0);
    },

    queuePct(q, key) {
      const total = this.queueTotal(q);
      if (total === 0) return "0%";
      return ((q[key] || 0) / total * 100).toFixed(1) + "%";
    },

    pctOf(value, total) {
      if (!total || total === 0) return "0%";
      return Math.min((value / total) * 100, 100).toFixed(1) + "%";
    },

    formatNumber(n) {
      if (n === undefined || n === null) return "0";
      if (n >= 1000000) return (n / 1000000).toFixed(1) + "M";
      if (n >= 1000) return (n / 1000).toFixed(1) + "K";
      return String(n);
    },

    formatTime(ts) {
      if (!ts) return "-";
      const d = new Date(ts * 1000);
      return d.toLocaleString(undefined, {
        month: "short", day: "numeric",
        hour: "2-digit", minute: "2-digit", second: "2-digit"
      });
    },

    formatTimeFull(ts) {
      if (!ts) return "-";
      const d = new Date(ts * 1000);
      return d.toLocaleString(undefined, {
        year: "numeric", month: "short", day: "numeric",
        hour: "2-digit", minute: "2-digit", second: "2-digit",
        fractionalSecondDigits: 3,
      });
    },

    timeAgo(ts) {
      if (!ts) return "-";
      const now = Date.now() / 1000;
      const diff = now - ts;
      if (diff < 5) return "just now";
      if (diff < 60) return Math.floor(diff) + "s ago";
      if (diff < 3600) return Math.floor(diff / 60) + "m ago";
      if (diff < 86400) return Math.floor(diff / 3600) + "h ago";
      return Math.floor(diff / 86400) + "d ago";
    },

    isScheduled(job) {
      if (!job || !job.delay_until) return false;
      if (job.status !== "pending") return false;
      return job.delay_until * 1000 > Date.now();
    },

    scheduledIn(ts) {
      if (!ts) return "";
      const diff = ts - Date.now() / 1000;
      if (diff <= 0) return "now";
      if (diff < 60) return `in ${Math.ceil(diff)}s`;
      if (diff < 3600) return `in ${Math.ceil(diff / 60)}m`;
      if (diff < 86400) return `in ${Math.ceil(diff / 3600)}h`;
      return `in ${Math.ceil(diff / 86400)}d`;
    },

    jobDuration(job) {
      if (!job.started_at) return "-";
      const end = job.completed_at || job.failed_at || (this.liveNow / 1000);
      return this.formatDuration(end - job.started_at);
    },

    formatDuration(diff) {
      if (!Number.isFinite(diff) || diff < 0) return "-";

      // Work in centiseconds so a live duration never displays more precision
      // than the requested hundredth of a second.
      const centiseconds = Math.floor(diff * 100);
      const totalSeconds = centiseconds / 100;
      if (totalSeconds < 60) return totalSeconds.toFixed(2) + "s";

      const minutes = Math.floor(totalSeconds / 60);
      const seconds = (totalSeconds % 60).toFixed(2).padStart(5, "0");
      if (minutes < 60) return minutes + "m " + seconds + "s";

      const hours = Math.floor(minutes / 60);
      const remainingMinutes = String(minutes % 60).padStart(2, "0");
      return hours + "h " + remainingMinutes + "m " + seconds + "s";
    },

    // ── Per-attempt timeline ────────────────────────────────

    attemptHistory(job) {
      return job && Array.isArray(job.history) ? job.history : [];
    },

    hasHistory(job) {
      return this.attemptHistory(job).length > 0;
    },

    // A job currently processing has an in-flight attempt that isn't recorded in
    // history yet (entries are appended only when an attempt concludes).
    inFlightAttempt(job) {
      return !!(job && job.status === "processing" && job.started_at);
    },

    attemptDotClass(entry) {
      return entry && entry.status === "completed" ? "completed" : "failed";
    },

    attemptDuration(entry) {
      if (!entry || !entry.started_at || !entry.finished_at) return "";
      const diff = entry.finished_at - entry.started_at;
      if (diff < 0) return "";
      if (diff < 0.001) return "<1ms";
      if (diff < 1) return Math.round(diff * 1000) + "ms";
      if (diff < 60) return diff.toFixed(1) + "s";
      return Math.floor(diff / 60) + "m " + Math.floor(diff % 60) + "s";
    },

    shortId(id) {
      return id ? id.substring(0, 12) : "-";
    },

    shortClass(cls) {
      if (!cls) return "-";
      const parts = cls.split(".");
      return parts[parts.length - 1];
    },
  }));
});
