// Main update function for deviations page
function updateAll() {
  const galleryId = document.getElementById("gallerySelect").value;
  const startDate = document.getElementById("startDate").value;
  const endDate = document.getElementById("endDate").value;

  const queryParams = new URLSearchParams({
    gallery: galleryId,
    start_date: startDate,
    end_date: endDate,
  }).toString();

  let offset = 0;
  const limit = 100;

  // Add scroll event listener
  window.addEventListener("scroll", () => {
    // Check if we're near the bottom
    if (
      window.innerHeight + window.scrollY >=
      document.documentElement.scrollHeight - 100
    ) {
      // Increment offset and fetch more
      offset += limit;

      const scrollQueryParams = new URLSearchParams({
        gallery: document.getElementById("gallerySelect").value,
        start_date: document.getElementById("startDate").value,
        end_date: document.getElementById("endDate").value,
        offset: offset,
        limit: limit,
      }).toString();

      fetch(`/get-deviations?${scrollQueryParams}`)
        .then((response) => response.json())
        .then((data) => {
          const deviationsContainer = document.getElementById(
            "deviations-container"
          );

          data.data.forEach((deviation) => {
            const deviationElement = document.createElement("div");
            deviationElement.classList.add("deviation", "grid-item");
            deviationElement.style.width = "150px";
            deviationElement.style.margin = "5px";
            deviationElement.style.position = "relative";
            deviationElement.innerHTML = `
              <a href="${deviation.url}" class="deviation-link">
                <img src="/thumbs/${deviation.deviationid}" alt="${
              deviation.title
            }" title="${deviation.title}" style="width: 100%; height: auto;"/>
                <div class="deviation-info" style="padding: 8px;">
                  <div style="font-size: 0.8em; color: #666;">
                    <span>❤️ ${JSON.parse(deviation.stats).favourites}</span>
                    <span style="margin-left: 8px;">💬 ${
                      JSON.parse(deviation.stats).comments
                    }</span>
                  </div>
                </div>
              </a>
            `;
            deviationsContainer.appendChild(deviationElement);
          });
        })
        .catch((error) => {
          console.error("Error fetching more deviations:", error);
        });
    }
  });

  fetch(`/get-deviations?${queryParams}`)
    .then((response) => response.json())
    .then((data) => {
      const deviationsContainer = document.getElementById(
        "deviations-container"
      );
      deviationsContainer.innerHTML = "";

      data.data.forEach((deviation) => {
        const deviationElement = document.createElement("div");
        deviationElement.classList.add("deviation", "grid-item");
        deviationElement.style.width = "150px";
        deviationElement.style.margin = "5px";
        deviationElement.style.position = "relative";
        deviationElement.innerHTML = `
          <a href="${deviation.url}" class="deviation-link">
            <img src="/thumbs/${deviation.deviationid}" alt="${
          deviation.title
        }" title="${deviation.title}" style="width: 100%; height: auto;"/>
            <div class="deviation-info" style="padding: 8px;">
              <div style="font-size: 0.8em; color: #666;">
                <span>❤️ ${JSON.parse(deviation.stats).favourites}</span>
                <span style="margin-left: 8px;">💬 ${
                  JSON.parse(deviation.stats).comments
                }</span>
              </div>
            </div>
          </a>
        `;
        if (!deviationsContainer.classList.contains("masonry-grid")) {
          deviationsContainer.classList.add("masonry-grid");
          deviationsContainer.style.display = "grid";
          deviationsContainer.style.gridAutoRows = "auto";
          deviationsContainer.style.alignItems = "start";
          deviationsContainer.style.gridTemplateColumns =
            "repeat(auto-fill, minmax(150px, 1fr))";
          deviationsContainer.style.gridGap = "10px";
          deviationsContainer.style.justifyItems = "center";
        }
        deviationsContainer.appendChild(deviationElement);
      });
    })
    .catch((error) => {
      console.error("Error fetching deviations:", error);
    });
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
