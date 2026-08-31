/* Progressive enhancement for the report site.
 *
 * Nothing here is required to read a page: every chart is server-rendered SVG,
 * every mark carries a native <title>, and every value also appears in a table.
 * This file only makes those things nicer to use — and it loads from the site's
 * own origin, so the page still needs no network beyond itself.
 */
(function () {
  "use strict";

  /* ---------- theme switch ---------- */
  // The stored choice is applied before first paint by a tiny inline script in
  // the document head; this only handles the click and keeps the two in step.
  function currentTheme() {
    var explicit = document.documentElement.getAttribute("data-theme");
    if (explicit) return explicit;
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  }

  function setTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    try {
      localStorage.setItem("mcb-theme", theme);
    } catch (e) {
      /* private browsing, or site data blocked: the switch still works for this page */
    }
  }

  Array.prototype.forEach.call(document.querySelectorAll("[data-theme-toggle]"), function (btn) {
    btn.addEventListener("click", function () {
      setTheme(currentTheme() === "dark" ? "light" : "dark");
    });
  });

  /* ---------- tooltips ---------- */
  // SVG's own <title> works but appears after a long delay and cannot be styled.
  // This replaces it on hover and on keyboard focus; the <title> stays in the
  // markup as the no-JS path and for screen readers.
  var tip = null;

  function ensureTip() {
    if (!tip) {
      tip = document.createElement("div");
      tip.className = "viz-tip";
      tip.setAttribute("role", "tooltip");
      document.body.appendChild(tip);
    }
    return tip;
  }

  function showTip(text, x, y) {
    var node = ensureTip();
    node.textContent = text;
    node.classList.add("is-visible");
    var box = node.getBoundingClientRect();
    var left = Math.min(Math.max(8, x + 12), window.innerWidth - box.width - 8);
    var top = y - box.height - 12;
    if (top < 8) top = y + 18;
    node.style.left = left + "px";
    node.style.top = top + "px";
  }

  function hideTip() {
    if (tip) tip.classList.remove("is-visible");
  }

  document.addEventListener(
    "mouseover",
    function (event) {
      var target = event.target.closest ? event.target.closest("[data-tip]") : null;
      if (!target) return;
      showTip(target.getAttribute("data-tip"), event.clientX, event.clientY);
    },
    true
  );

  document.addEventListener(
    "mousemove",
    function (event) {
      if (!tip || !tip.classList.contains("is-visible")) return;
      var target = event.target.closest ? event.target.closest("[data-tip]") : null;
      if (!target) {
        hideTip();
        return;
      }
      showTip(target.getAttribute("data-tip"), event.clientX, event.clientY);
    },
    true
  );

  document.addEventListener("mouseout", function (event) {
    var target = event.target.closest ? event.target.closest("[data-tip]") : null;
    if (target) hideTip();
  });

  window.addEventListener("scroll", hideTip, true);

  /* ---------- legend focus ---------- */
  // Hovering a legend entry dims the other series. Colour never changes: a
  // reader who learned that gmqtt is blue must not see it repainted.
  Array.prototype.forEach.call(document.querySelectorAll(".legend"), function (legend) {
    var wrap = legend.closest(".chart-wrap") || legend.parentNode;
    if (!wrap) return;

    function focus(name) {
      var marks = wrap.querySelectorAll("[data-series]");
      if (!marks.length) return;
      wrap.classList.toggle("is-focused", !!name);
      Array.prototype.forEach.call(marks, function (mark) {
        mark.classList.toggle("is-focus", !name || mark.getAttribute("data-series") === name);
      });
    }

    Array.prototype.forEach.call(legend.querySelectorAll("[data-series]"), function (item) {
      item.addEventListener("mouseenter", function () {
        focus(item.getAttribute("data-series"));
      });
      item.addEventListener("mouseleave", function () {
        focus(null);
      });
    });
  });

  /* ---------- sortable tables ---------- */
  // Sorting is a convenience on top of an order the generator already chose
  // deliberately, so the first click sorts descending for numbers (largest
  // first is what a reader wants from a ranking) and ascending for text.
  function cellValue(row, index, kind) {
    var cell = row.cells[index];
    if (!cell) return kind === "num" ? Number.NEGATIVE_INFINITY : "";
    var text = cell.textContent.trim();
    if (kind !== "num") return text.toLowerCase();
    var cleaned = text.replace(/[\s,%×]/g, "").replace(/[^0-9.eE+-].*$/, "");
    var value = parseFloat(cleaned);
    return isNaN(value) ? Number.NEGATIVE_INFINITY : value;
  }

  Array.prototype.forEach.call(document.querySelectorAll("table"), function (tableEl) {
    var head = tableEl.tHead;
    var body = tableEl.tBodies[0];
    if (!head || !body) return;

    Array.prototype.forEach.call(head.querySelectorAll("th[data-sort]"), function (th, index) {
      th.addEventListener("click", function () {
        var kind = th.getAttribute("data-sort");
        var ascending = th.getAttribute("aria-sort") === "descending";
        if (!th.hasAttribute("aria-sort")) ascending = kind !== "num";

        Array.prototype.forEach.call(head.querySelectorAll("th[aria-sort]"), function (other) {
          other.removeAttribute("aria-sort");
        });
        th.setAttribute("aria-sort", ascending ? "ascending" : "descending");

        var rows = Array.prototype.slice.call(body.rows);
        rows.sort(function (a, b) {
          var av = cellValue(a, index, kind);
          var bv = cellValue(b, index, kind);
          if (av < bv) return ascending ? -1 : 1;
          if (av > bv) return ascending ? 1 : -1;
          return 0;
        });
        rows.forEach(function (row) {
          body.appendChild(row);
        });
      });
    });
  });
})();
