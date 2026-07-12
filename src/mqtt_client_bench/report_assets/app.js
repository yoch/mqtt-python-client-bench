(() => {
  const palette = ["#0f6e56", "#245b7a", "#9a5b12", "#3f6b4d", "#6b4f7a"];

  function parse(el, name) {
    try {
      return JSON.parse(el.getAttribute(name) || "[]");
    } catch (_) {
      return [];
    }
  }

  function makeChart(canvas, labels, values, label) {
    if (!canvas || typeof Chart === "undefined") return;
    const cleanedLabels = labels.length ? labels : ["No data"];
    const cleanedValues = values.length ? values.map((v) => (v == null ? null : Number(v))) : [null];
    new Chart(canvas, {
      type: "bar",
      data: {
        labels: cleanedLabels,
        datasets: [
          {
            label,
            data: cleanedValues,
            backgroundColor: cleanedLabels.map((_, i) => palette[i % palette.length]),
            borderRadius: 10,
            maxBarThickness: 48,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              afterBody(items) {
                const idx = items[0]?.dataIndex;
                const clients = parse(canvas, "data-clients");
                if (clients[idx]) return [`client: ${clients[idx]}`];
                return [];
              },
            },
          },
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

  const overview = document.getElementById("overview-chart");
  if (overview) {
    makeChart(overview, parse(overview, "data-labels"), parse(overview, "data-values"), "Median msg/s");
  }

  document.querySelectorAll(".detail-chart").forEach((canvas) => {
    makeChart(canvas, parse(canvas, "data-labels"), parse(canvas, "data-values"), "Median msg/s");
  });
})();
