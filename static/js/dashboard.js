// Chart initialization and update functions
function updatePublicationChart() {
  // Show loading spinner while fetching data
  const existingChart = Chart.getChart("publicationChart");
  if (existingChart) {
    existingChart.destroy();
  }

  const ctx = document.getElementById("publicationChart").getContext("2d");
  ctx.save();
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.font = "16px Arial";
  ctx.fillStyle = "#6c757d";

  // Clear canvas and draw loading text/spinner
  ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
  ctx.fillText("Loading...", ctx.canvas.width / 2, ctx.canvas.height / 2);
  ctx.restore();

  const startDate = document.getElementById("startDate").value;
  const endDate = document.getElementById("endDate").value;
  const gallery = document.getElementById("gallerySelect").value;

  const queryParams = new URLSearchParams({
    start_date: startDate,
    end_date: endDate,
    gallery: gallery,
  }).toString();

  fetch(`/get-publication-data?${queryParams}`)
    .then((response) => response.json())
    .then((data) => {
      console.log(data);
      // Fill in any missing dates with 0 counts
      const firstDate = startDate
        ? new Date(startDate)
        : new Date(data.data[0].date);
      const lastDate = endDate
        ? new Date(endDate)
        : new Date(data.data[data.data.length - 1].date);
      const filledData = [];

      let currentDate = new Date(firstDate);
      while (currentDate <= lastDate) {
        const existingData = data.data.find(
          (d) => new Date(d.date).toDateString() === currentDate.toDateString()
        );

        filledData.push({
          date: currentDate.toISOString().split("T")[0],
          count: existingData ? existingData.deviations : 0,
          favorites: existingData ? existingData.favorites : 0,
        });

        currentDate.setDate(currentDate.getDate() + 1);
      }

      data.data = filledData;

      // Create new chart with zoom plugin
      const chart = new Chart("publicationChart", {
        type: "bar",
        plugins: [
          {
            id: "dragSelect",
            afterInit: function (chart) {
              let dragStart = null;
              let dragging = false;

              chart.canvas.addEventListener("mousedown", (e) => {
                const rect = chart.canvas.getBoundingClientRect();
                dragStart = {
                  x: e.clientX - rect.left,
                  y: e.clientY - rect.top,
                };
                dragging = true;
              });

              chart.canvas.addEventListener("mousemove", (e) => {
                if (!dragging) return;

                const rect = chart.canvas.getBoundingClientRect();
                const x = e.clientX - rect.left;

                // Get dates from x coordinates
                const xScale = chart.scales.x;
                const startIndex = xScale.getValueForPixel(
                  Math.min(dragStart.x, x)
                );
                const endIndex = xScale.getValueForPixel(
                  Math.max(dragStart.x, x)
                );

                const startDate =
                  data.data[Math.max(0, Math.floor(startIndex))].date;
                const endDate =
                  data.data[Math.min(data.data.length - 1, Math.ceil(endIndex))]
                    .date;

                // Update date inputs
                document.getElementById("startDate").value = startDate;
                document.getElementById("endDate").value = endDate;
              });

              document.addEventListener("mouseup", () => {
                if (dragging) {
                  dragging = false;
                  updateAll();
                }
              });
            },
          },
        ],
        data: {
          labels: data.data.map((d) => new Date(d.date).toLocaleDateString()),
          datasets: [
            {
              label: "Number of Deviations",
              data: data.data.map((d) => d.count),
              backgroundColor: "rgba(54, 162, 235, 0.5)",
              borderColor: "rgba(54, 162, 235, 1)",
            },
            {
              label: "Number of Favorites",
              data: data.data.map((d) => d.favorites),
              backgroundColor: "rgba(255, 159, 64, 0.5)",
              borderColor: "rgba(255, 159, 64, 1)",
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: { y: { beginAtZero: true } },
        },
      });
    });
}

// Table sorting functionality
function sortTable(n) {
  var table,
    rows,
    switching,
    i,
    x,
    y,
    shouldSwitch,
    dir,
    switchcount = 0;
  table = document.querySelector(".table");
  switching = true;
  dir = "asc";

  while (switching) {
    switching = false;
    rows = table.rows;

    for (i = 1; i < rows.length - 1; i++) {
      shouldSwitch = false;
      x = rows[i].getElementsByTagName("TD")[n];
      y = rows[i + 1].getElementsByTagName("TD")[n];

      if (n >= 2) {
        // For numeric columns
        if (dir == "asc") {
          if (Number(x.innerHTML) > Number(y.innerHTML)) {
            shouldSwitch = true;
            break;
          }
        } else {
          if (Number(x.innerHTML) < Number(y.innerHTML)) {
            shouldSwitch = true;
            break;
          }
        }
      } else {
        // For text columns
        if (dir == "asc") {
          if (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) {
            shouldSwitch = true;
            break;
          }
        } else {
          if (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) {
            shouldSwitch = true;
            break;
          }
        }
      }
    }

    if (shouldSwitch) {
      rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
      switching = true;
      switchcount++;
    } else {
      if (switchcount == 0 && dir == "asc") {
        dir = "desc";
        switching = true;
      }
    }
  }
}

// Function to update the table
function updateTable() {
  const startDate = document.getElementById("startDate").value;
  const endDate = document.getElementById("endDate").value;
  const limit = document.getElementById("limitSelect").value;
  const gallery = document.getElementById("gallerySelect").value;
  const selectedGallery =
    document.getElementById("gallerySelect").options[
      document.getElementById("gallerySelect").selectedIndex
    ].text;

  title = `Top Deviations`;
  if (startDate) {
    title += ` ${startDate}`;
  }
  if (endDate) {
    title += ` - ${endDate}`;
  }

  if (gallery) {
    title += ` - ${selectedGallery}`;
  }

  document.getElementById("deviationTitle").innerHTML = title;

  // Show loading spinner while fetching data
  document.getElementById("deviationTable").innerHTML = `
    <tr>
      <td colspan="3" class="text-center">
        <div class="spinner-border text-primary" role="status">
          <span class="visually-hidden">Loading...</span>
        </div>
      </td>
    </tr>
  `;

  const queryParams = new URLSearchParams({
    start_date: startDate,
    end_date: endDate,
    limit: limit,
    gallery: gallery,
  }).toString();

  fetch(`/update-table?${queryParams}`)
    .then((response) => response.json())
    .then((data) => {
      const header = document.getElementById("deviationTableHeader");
      header.innerHTML = "";
      if (!startDate && !endDate) {
        header.innerHTML = `
          <tr>
            <th></th>
            <th class="sortable" onclick="sortTable(1)">Title</th>
            <th class="sortable" onclick="sortTable(2)">Favorites</th>
            <th class="sortable" onclick="sortTable(3)">Views</th>
            <th class="sortable" onclick="sortTable(4)">Comments</th>
            <th class="sortable" onclick="sortTable(5)">Downloads</th>
          </tr>
        `;
      } else {
        header.innerHTML = `
          <tr>
            <th></th>
            <th class="sortable" onclick="sortTable(1)">Title</th>
            <th class="sortable" onclick="sortTable(2)">Favorites</th>
            <th></th>
          </tr>
        `;
      }

      const table = document.getElementById("deviationTable");
      table.innerHTML = "";

      data.data.forEach((row) => {
        const tr = document.createElement("tr");

        if (!startDate && !endDate) {
          tr.innerHTML = `
            <td><img src="/thumbs/${row.deviationid}" alt="thumbnail" style="width: 50px;"></td>
            <td><a class="deviation-link" href="${row.url}" target="_blank">${row.title}</a></td>
            <td>${row.favorites}</td>
            <td>${row.views}</td>
            <td>${row.comments}</td>
            <td>${row.downloads}</td>
          `;
          table.appendChild(tr);
        } else {
          tr.innerHTML = `
            <td><img src="/thumbs/${row.deviationid}" alt="Thumbnail" style="width: 50px;"></td>
            <td><a href="${row.url}" target="_blank">${row.title}</a></td>
            <td>${row.favorites}</td>
            <td id="sparkline-${row.deviationid}"></td>
          `;
          table.appendChild(tr);
          getSparklineData(
            row.deviationid,
            document.getElementById(`sparkline-${row.deviationid}`)
          );
        }
      });
    });
}

// Function to fetch and display sparkline data for a deviation
function getSparklineData(deviationId, element) {
  const startDate = document.getElementById("startDate").value;
  const endDate = document.getElementById("endDate").value;

  fetch("/get-sparkline-data", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      id: deviationId,
      start_date: startDate,
      end_date: endDate,
    }),
  })
    .then((response) => response.json())
    .then((data) => {
      if (data.status === "success") {
        // Create sparkline using data.data
        const sparklineData = data.data.map((d) => d.count || 0);

        // Clear any existing content
        element.innerHTML = "";

        // Create sparkline chart
        const sparkline = document.createElement("canvas");
        sparkline.style.width = "100px";
        element.appendChild(sparkline);

        new Chart(sparkline, {
          type: "line",
          data: {
            labels: data.data.map((d) =>
              new Date(d.timestamp).toLocaleDateString()
            ),
            datasets: [
              {
                data: sparklineData,
                borderColor: "rgba(54, 162, 235, 1)",
                borderWidth: 1,
                fill: false,
                pointRadius: 0,
                pointHoverRadius: 4,
                pointHoverBackgroundColor: "rgba(54, 162, 235, 1)",
                pointHoverBorderColor: "rgba(54, 162, 235, 1)",
                pointHitRadius: 10,
              },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: {
                display: false,
              },
            },
            scales: {
              x: {
                display: false,
              },
              y: {
                display: false,
              },
            },
          },
        });
      }
    })
    .catch((error) => {
      console.error("Error fetching sparkline data:", error);
      element.innerHTML = '<span class="text-danger">Error</span>';
    });
}

// Function to fetch and display top users data
function topUsers() {
  // Show loading spinner while fetching data
  document.getElementById("userTable").innerHTML = `
    <tr>
      <td colspan="2" class="text-center">
        <div class="spinner-border text-primary" role="status">
          <span class="visually-hidden">Loading...</span>
        </div>
      </td>
    </tr>
  `;

  const startDate = document.getElementById("startDate").value;
  const endDate = document.getElementById("endDate").value;
  const limit = document.getElementById("topUserlimitSelect").value;
  const gallery = document.getElementById("gallerySelect").value;
  const queryParams = new URLSearchParams({
    start_date: startDate,
    end_date: endDate,
    limit: limit,
    gallery: gallery,
  }).toString();

  fetch(`/get-users?${queryParams}`)
    .then((response) => response.json())
    .then((data) => {
      if (data.status === "success") {
        const userTable = document.getElementById("userTable");
        userTable.innerHTML = "";

        data.data.forEach((user) => {
          const row = document.createElement("tr");
          row.innerHTML = `
            <td><img src="${user.usericon}" alt="User Icon""></td>
            <td><a class="user-link" href="https://www.deviantart.com/${user.username}" target="_blank">${user.username}</a></td>
            <td>${user.favorites}</td>
          `;
          userTable.appendChild(row);
        });
      }
    })
    .catch((error) => {
      console.error("Error fetching top users:", error);
      document.getElementById("userTable").innerHTML = `
        <tr>
          <td colspan="2" class="text-center text-danger">
            Error loading user data
          </td>
        </tr>
      `;
    });
}

// Main update function
function updateAll() {
  updatePublicationChart();
  updateTable();
  topUsers();
}

// Initialize on page load
document.addEventListener("DOMContentLoaded", function () {
  // Initialize date pickers
  flatpickr("#startDate", {
    enableTime: false,
    dateFormat: "Y-m-d",
    onChange: function (date) {
      updateAll();
    },
  });

  flatpickr("#endDate", {
    enableTime: false,
    dateFormat: "Y-m-d",
    onChange: function (date) {
      updateAll();
    },
  });

  // Load gallery names
  fetch("/get-gallery-names")
    .then((response) => response.json())
    .then((data) => {
      const select = document.getElementById("gallerySelect");
      data.data.forEach((gallery) => {
        const option = document.createElement("option");
        option.value = gallery.folderid;
        option.textContent = gallery.name;
        select.appendChild(option);
      });
    });

  // Initial update
  updateAll();
});
