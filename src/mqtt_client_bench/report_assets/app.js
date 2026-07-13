(() => {
  const palette = ["#0f6e56", "#245b7a", "#9a5b12", "#6b4f7a", "#8b3a3a", "#3f6b4d"];

  // Draws min/max whiskers over each bar from a per-dataset `errorRanges`
  // array of [low, high] pairs (indices matching the dataset's data points).
  // Chart.js has no built-in error-bar support, so this plugin fills that gap
  // without pulling in an extra dependency.
  const errorBarsPlugin = {
    id: "errorBars",
    afterDatasetsDraw(chart) {
      const { ctx } = chart;
      chart.data.datasets.forEach((dataset, datasetIndex) => {
        const ranges = dataset.errorRanges;
        if (!ranges) return;
        const meta = chart.getDatasetMeta(datasetIndex);
        if (!meta || meta.hidden) return;
        const yScale = chart.scales[meta.yAxisID || "y"];
        if (!yScale) return;
        meta.data.forEach((element, index) => {
          const range = ranges[index];
          if (!range) return;
          const [low, high] = range;
          if (low == null || high == null || Number.isNaN(low) || Number.isNaN(high)) return;
          const x = element.x;
          const yLow = yScale.getPixelForValue(low);
          const yHigh = yScale.getPixelForValue(high);
          const capWidth = Math.max(4, Math.min(9, (element.width || 20) / 2.5));
          ctx.save();
          ctx.strokeStyle = "rgba(28, 42, 36, 0.6)";
          ctx.lineWidth = 1.5;
          ctx.beginPath();
          ctx.moveTo(x, yHigh);
          ctx.lineTo(x, yLow);
          ctx.moveTo(x - capWidth, yHigh);
          ctx.lineTo(x + capWidth, yHigh);
          ctx.moveTo(x - capWidth, yLow);
          ctx.lineTo(x + capWidth, yLow);
          ctx.stroke();
          ctx.restore();
        });
      });
    },
  };
  if (typeof Chart !== "undefined") {
    Chart.register(errorBarsPlugin);
  }

  function parse(el, name) {
    try {
      return JSON.parse(el.getAttribute(name) || "[]");
    } catch (_) {
      return [];
    }
  }

  function makeDetailChart(canvas, labels, values, label, lows, highs) {
    if (!canvas || typeof Chart === "undefined") return;
    const cleanedLabels = labels.length ? labels : ["No data"];
    const cleanedValues = values.length ? values.map((v) => (v == null ? null : Number(v))) : [null];
    const errorRanges =
      lows && highs && lows.length === cleanedValues.length
        ? cleanedValues.map((_, i) => [lows[i], highs[i]])
        : undefined;
    new Chart(canvas, {
      type: "bar",
      data: {
        labels: cleanedLabels,
        datasets: [
          {
            label,
            data: cleanedValues,
            backgroundColor: "#0f6e56",
            borderRadius: 8,
            maxBarThickness: 48,
            errorRanges,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
        },
        scales: {
          x: {
            ticks: { maxRotation: 45, minRotation: 0, color: "#5c6b64" },
            grid: { display: false },
          },
          y: {
            beginAtZero: true,
            ticks: { color: "#5c6b64" },
            grid: { color: "rgba(28,42,36,0.08)" },
            title: { display: true, text: "messages / s", color: "#5c6b64" },
          },
        },
      },
    });
  }

  function makeOverviewChart(canvas, payload) {
    if (!canvas || typeof Chart === "undefined") return;
    const scenarios = payload.scenarios || [];
    const series = payload.series || [];
    if (!scenarios.length || !series.length) {
      makeDetailChart(canvas, [], [], "Median msg/s");
      return;
    }
    new Chart(canvas, {
      type: "bar",
      data: {
        labels: scenarios,
        datasets: series.map((s, i) => {
          const values = (s.values || []).map((v) => (v == null ? null : Number(v)));
          const errorRanges =
            s.low && s.high && s.low.length === values.length ? values.map((_, j) => [s.low[j], s.high[j]]) : undefined;
          return {
            label: s.client,
            data: values,
            backgroundColor: s.color || palette[i % palette.length],
            borderRadius: 6,
            maxBarThickness: 28,
            errorRanges,
          };
        }),
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: {
            display: true,
            position: "top",
            align: "start",
            labels: {
              boxWidth: 14,
              boxHeight: 14,
              color: "#1c2a24",
              font: { family: "'IBM Plex Sans', sans-serif", size: 13 },
            },
          },
          tooltip: {
            callbacks: {
              title(items) {
                return items[0]?.label || "";
              },
              label(item) {
                const v = item.raw;
                if (v == null || Number.isNaN(v)) return `${item.dataset.label}: —`;
                return `${item.dataset.label}: ${Number(v).toLocaleString(undefined, { maximumFractionDigits: 0 })} msg/s`;
              },
            },
          },
        },
        scales: {
          x: {
            ticks: {
              maxRotation: 55,
              minRotation: 55,
              color: "#5c6b64",
              autoSkip: false,
              font: { size: 11 },
            },
            grid: { display: false },
            title: { display: true, text: "scenario", color: "#5c6b64" },
          },
          y: {
            beginAtZero: true,
            ticks: {
              color: "#5c6b64",
              callback: (v) => Number(v).toLocaleString(),
            },
            grid: { color: "rgba(28,42,36,0.08)" },
            title: { display: true, text: "messages / s", color: "#5c6b64" },
          },
        },
      },
    });
  }

  const overview = document.getElementById("overview-chart");
  if (overview) {
    const payload = parse(overview, "data-overview");
    if (payload && payload.scenarios) {
      makeOverviewChart(overview, payload);
    } else {
      // Backward-compatible fallback for older generated pages.
      makeDetailChart(
        overview,
        parse(overview, "data-labels"),
        parse(overview, "data-values"),
        "Median msg/s"
      );
    }
  }

  document.querySelectorAll(".detail-chart").forEach((canvas) => {
    makeDetailChart(
      canvas,
      parse(canvas, "data-labels"),
      parse(canvas, "data-values"),
      "Median msg/s",
      parse(canvas, "data-low"),
      parse(canvas, "data-high")
    );
  });
})();
