/* Only One Suivi autonome : remplace le moteur de Claude par Supabase.
   Expose window.claude.use("db" | "downloads") avec la même API que l'appli utilisait. */
(function () {
  var CFG = window.OO_CONFIG || {};
  var sb = null;
  var listeners = {}; // col -> Set(fn)
  var cache = {}; // col -> Map(id -> data)
  var loaded = {}; // col -> Promise
  var channel = null;

  function newId() {
    var a = "abcdefghijklmnopqrstuvwxyz0123456789", s = "";
    var r = crypto.getRandomValues(new Uint8Array(20));
    for (var i = 0; i < 20; i++) s += a[r[i] % 36];
    return s;
  }
  function errOf(e) {
    var x = new Error((e && e.message) || "Erreur de la base de données");
    x.code = e && e.code === "PGRST301" ? "unauthenticated" : "db_error";
    return x;
  }
  function snapCol(col) {
    var m = cache[col] || new Map();
    var docs = [];
    m.forEach(function (data, id) {
      docs.push({ id: id, data: function () { return data; }, exists: true });
    });
    return { docs: docs, size: docs.length, empty: docs.length === 0 };
  }
  function snapDoc(col, id) {
    var m = cache[col];
    var data = m && m.get(id);
    return { id: id, exists: !!data, data: function () { return data; } };
  }
  function emit(col) {
    var set = listeners[col];
    if (set) set.forEach(function (fn) { try { fn(); } catch (e) { console.error(e); } });
  }
  async function loadCol(col) {
    if (loaded[col]) return loaded[col];
    loaded[col] = (async function () {
      var m = new Map(), from = 0, step = 1000;
      for (;;) {
        var res = await sb.from("docs").select("id,data").eq("col", col).range(from, from + step - 1);
        if (res.error) { delete loaded[col]; throw errOf(res.error); }
        res.data.forEach(function (r) { m.set(r.id, r.data || {}); });
        if (res.data.length < step) break;
        from += step;
      }
      cache[col] = m;
    })();
    return loaded[col];
  }
  function ensureChannel() {
    if (channel) return;
    channel = sb.channel("docs-all").on("postgres_changes", { event: "*", schema: "public", table: "docs" }, function (p) {
      var row = p.new && p.new.col ? p.new : p.old || {};
      var col = row.col, id = row.id;
      if (!col || !cache[col]) {
        // suppression sans colonne : on cherche dans toutes les collections chargées
        if (p.eventType === "DELETE" && p.old && p.old.id) {
          Object.keys(cache).forEach(function (c) { if (cache[c].delete(p.old.id)) emit(c); });
        }
        return;
      }
      if (p.eventType === "DELETE") cache[col].delete(id);
      else cache[col].set(id, p.new.data || {});
      emit(col);
    }).subscribe(function (status) {
      if (status === "CHANNEL_ERROR" || status === "TIMED_OUT") {
        // reprise : on recharge tout ce qui est écouté
        setTimeout(function () {
          channel && sb.removeChannel(channel); channel = null;
          Object.keys(loaded).forEach(function (c) { delete loaded[c]; });
          Object.keys(listeners).forEach(function (c) { loadCol(c).then(function () { emit(c); }); });
          ensureChannel();
        }, 3000);
      }
    });
  }
  function listen(col, fn, onErr) {
    ensureChannel();
    (listeners[col] = listeners[col] || new Set()).add(fn);
    loadCol(col).then(function () { fn(); }, function (e) { onErr && onErr(e); });
    return function () { listeners[col] && listeners[col].delete(fn); };
  }
  function local(col, id, data) {
    if (cache[col]) {
      if (data === null) cache[col].delete(id); else cache[col].set(id, data);
      emit(col);
    }
  }
  function docRef(col, id) {
    return {
      id: id,
      set: async function (data) {
        var res = await sb.from("docs").upsert({ col: col, id: id, data: data || {} }, { onConflict: "col,id" });
        if (res.error) throw errOf(res.error);
        local(col, id, data || {});
      },
      update: async function (patch) {
        var res = await sb.rpc("docs_merge", { p_col: col, p_id: id, p_patch: patch || {} });
        if (res.error) throw errOf(res.error);
        var cur = (cache[col] && cache[col].get(id)) || {};
        var next = Object.assign({}, cur, patch);
        Object.keys(next).forEach(function (k) { if (next[k] && next[k].__delete__) delete next[k]; });
        local(col, id, next);
      },
      delete: async function () {
        var res = await sb.from("docs").delete().eq("col", col).eq("id", id);
        if (res.error) throw errOf(res.error);
        local(col, id, null);
      },
      get: async function () {
        var res = await sb.from("docs").select("data").eq("col", col).eq("id", id).maybeSingle();
        if (res.error) throw errOf(res.error);
        var d = res.data && res.data.data;
        return { id: id, exists: !!d, data: function () { return d; } };
      },
      onSnapshot: function (fn, onErr) {
        return listen(col, function () { fn(snapDoc(col, id)); }, onErr);
      }
    };
  }
  var db = {
    collection: function (col) {
      return {
        doc: function (id) { return docRef(col, id); },
        add: async function (data) {
          var id = newId();
          await docRef(col, id).set(data);
          return { id: id };
        },
        onSnapshot: function (fn, onErr) {
          return listen(col, function () { fn(snapCol(col)); }, onErr);
        }
      };
    },
    doc: function (path) {
      var p = String(path).split("/");
      return docRef(p[0], p[1]);
    }
  };
  var downloads = {
    save: async function (o) {
      var blob = o.data instanceof Blob ? o.data : new Blob([o.data], { type: o.mimeType || "text/csv;charset=utf-8" });
      var a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = o.filename || "export";
      document.body.appendChild(a);
      a.click();
      setTimeout(function () { URL.revokeObjectURL(a.href); a.remove(); }, 1500);
    }
  };

  /* ----- Connexion ----- */
  var readyResolve;
  var dbReady = new Promise(function (r) { readyResolve = r; });

  function loginScreen(msg) {
    var el = document.getElementById("oo-login");
    if (!el) {
      el = document.createElement("div");
      el.id = "oo-login";
      document.body.appendChild(el);
    }
    el.innerHTML = '<form id="oo-lf" novalidate><div class="oo-brand">ONLY <span>◆</span> ONE</div><div class="oo-sub">Suivi</div>' +
      '<label>Email<input id="oo-em" type="email" autocomplete="username" required></label>' +
      '<label>Mot de passe<input id="oo-pw" type="password" autocomplete="current-password" required></label>' +
      '<p id="oo-msg">' + (msg || "") + '</p><button type="submit">Se connecter</button>' +
      '<button type="button" id="oo-forgot" class="oo-link">Mot de passe oublié</button></form>';
    document.getElementById("oo-lf").addEventListener("submit", async function (e) {
      e.preventDefault();
      var m = document.getElementById("oo-msg");
      m.textContent = "Connexion…";
      var r = await sb.auth.signInWithPassword({ email: document.getElementById("oo-em").value.trim(), password: document.getElementById("oo-pw").value });
      if (r.error) { m.textContent = "Email ou mot de passe incorrect."; return; }
      el.remove();
      openDb();
    });
    document.getElementById("oo-forgot").addEventListener("click", async function () {
      var em = document.getElementById("oo-em").value.trim(), m = document.getElementById("oo-msg");
      if (!em) { m.textContent = "Écris d'abord ton email."; return; }
      var r = await sb.auth.resetPasswordForEmail(em, { redirectTo: location.origin + location.pathname });
      m.textContent = r.error ? "L'envoi n'a pas abouti." : "Un lien t'a été envoyé par email.";
    });
  }
  function newPasswordScreen() {
    var el = document.createElement("div");
    el.id = "oo-login";
    el.innerHTML = '<form id="oo-np" novalidate><div class="oo-brand">ONLY <span>◆</span> ONE</div><div class="oo-sub">Nouveau mot de passe</div>' +
      '<label>Mot de passe<input id="oo-np1" type="password" autocomplete="new-password" minlength="8" required></label><p id="oo-msg"></p><button type="submit">Enregistrer</button></form>';
    document.body.appendChild(el);
    document.getElementById("oo-np").addEventListener("submit", async function (e) {
      e.preventDefault();
      var pw = document.getElementById("oo-np1").value;
      if (pw.length < 8) { document.getElementById("oo-msg").textContent = "8 caractères minimum."; return; }
      var r = await sb.auth.updateUser({ password: pw });
      if (r.error) { document.getElementById("oo-msg").textContent = "Échec, réessaie."; return; }
      el.remove();
      openDb();
    });
  }
  /* Première ouverture : base vide, on propose d'importer le fichier de données */
  async function openDb() {
    var c = await sb.from("docs").select("id", { count: "exact", head: true });
    if (!c.error && c.count === 0) return importScreen();
    readyResolve(db);
  }
  function importScreen() {
    var el = document.createElement("div");
    el.id = "oo-login";
    el.innerHTML = '<form id="oo-imp" novalidate><div class="oo-brand">ONLY <span>◆</span> ONE</div><div class="oo-sub">Bienvenue</div>' +
      '<p id="oo-msg" style="text-align:left">Ta base est prête mais vide. Choisis le fichier <b>only-one-donnees.json</b> que Claude t\'a envoyé pour y copier tes clients, prestas et prospects.</p>' +
      '<label>Fichier de données<input id="oo-file" type="file" accept="application/json,.json"></label>' +
      '<button type="submit">Importer mes données</button><button type="button" id="oo-skip" class="oo-link">Commencer avec une base vide</button></form>';
    document.body.appendChild(el);
    document.getElementById("oo-skip").addEventListener("click", function () { el.remove(); readyResolve(db); });
    document.getElementById("oo-imp").addEventListener("submit", async function (e) {
      e.preventDefault();
      var m = document.getElementById("oo-msg"), f = document.getElementById("oo-file").files[0];
      if (!f) { m.textContent = "Choisis d'abord le fichier."; return; }
      var j;
      try { j = JSON.parse(await f.text()); } catch (er) { m.textContent = "Ce fichier n'est pas lisible."; return; }
      var rows = [], cols = (j && j.collections) || {};
      Object.keys(cols).forEach(function (col) {
        Object.keys(cols[col]).forEach(function (id) { rows.push({ col: col, id: id, data: cols[col][id] }); });
      });
      if (!rows.length) { m.textContent = "Le fichier ne contient aucune donnée."; return; }
      for (var i = 0; i < rows.length; i += 200) {
        m.textContent = "Import en cours… " + Math.min(i + 200, rows.length) + " sur " + rows.length;
        var r = await sb.from("docs").upsert(rows.slice(i, i + 200), { onConflict: "col,id" });
        if (r.error) { m.textContent = "Erreur pendant l'import : " + r.error.message; return; }
      }
      m.textContent = rows.length + " fiches importées.";
      setTimeout(function () { el.remove(); readyResolve(db); }, 700);
    });
  }
  async function start() {
    if (!window.supabase || !CFG.url || !CFG.anonKey) {
      readyResolve(null);
      return;
    }
    sb = window.supabase.createClient(CFG.url, CFG.anonKey, { auth: { persistSession: true, autoRefreshToken: true, detectSessionInUrl: true } });
    window.OO_SB = sb;
    var recovery = /type=recovery/.test(location.hash);
    sb.auth.onAuthStateChange(function (ev) {
      if (ev === "PASSWORD_RECOVERY") newPasswordScreen();
      if (ev === "SIGNED_OUT") location.reload();
    });
    var s = await sb.auth.getSession();
    if (recovery) return;
    if (s.data && s.data.session) openDb();
    else loginScreen("");
  }
  window.OO_LOGOUT = function () { sb && sb.auth.signOut(); };
  window.OO_NEWPW = function () { newPasswordScreen(); };
  window.OO_STANDALONE = true;
  window.claude = {
    use: async function (name) {
      if (name === "db") return dbReady;
      if (name === "downloads") return downloads;
      return null;
    }
  };
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
  else start();
})();
