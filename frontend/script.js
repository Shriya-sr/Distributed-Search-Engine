
const searchForm = document.getElementById("search-form");
const searchInput = document.getElementById("search-input");
const searchButton = document.getElementById("search-button");
const versionSelect = document.getElementById("version-select");

const resultsSection = document.getElementById("results-section");
const resultsContainer = document.getElementById("results-container");
const resultCount = document.getElementById("result-count");

const welcomeMessage = document.getElementById("welcome-message");
const loadingMessage = document.getElementById("loading-message");
const errorMessage = document.getElementById("error-message");

const systemStatus = document.getElementById("system-status");
const statusDot = document.querySelector(".status-dot");

const API_BASE = "";

// Discover available index versions from FastAPI.
async function loadVersions() {
    try {
        const response = await fetch(`${API_BASE}/versions`);

        if (!response.ok) {
            throw new Error("Version discovery failed");
        }

        const data = await response.json();
        const versions = data.available_versions || [];

        versionSelect.replaceChildren();

        for (const version of versions) {
            const option = document.createElement("option");
            option.value = String(version);
            option.textContent = `Phase ${version}`;
            versionSelect.appendChild(option);
        }

        if (versions.length > 0) {
            versionSelect.value = String(data.default_version);
            versionSelect.disabled = false;
        } else {
            versionSelect.add(new Option("No versions available", ""));
            versionSelect.disabled = true;
        }
    } catch (error) {
        versionSelect.replaceChildren();
        const option = document.createElement("option");
        option.value = "";
        option.textContent = "Default version";
        versionSelect.appendChild(option);
        versionSelect.disabled = true;
    }
}

// Check whether the backend is available.
async function checkHealth() {
    try {
        const response = await fetch(`${API_BASE}/health`);

        if (!response.ok) {
            throw new Error("Backend unavailable");
        }

        const data = await response.json();

        systemStatus.textContent = `System Online · v${data.version}`;
        statusDot.classList.add("online");
        statusDot.classList.remove("offline");
    } catch (error) {
        systemStatus.textContent = "System Offline";
        statusDot.classList.add("offline");
        statusDot.classList.remove("online");
    }
}

// Show or hide interface elements.
function setLoading(isLoading) {
    loadingMessage.classList.toggle("hidden", !isLoading);
    searchButton.disabled = isLoading;

    searchButton.innerHTML = isLoading
        ? "<span>Searching...</span>"
        : "<span>Search</span><span>→</span>";
}

// Display an error message.
function showError(message) {
    errorMessage.textContent = message;
    errorMessage.classList.remove("hidden");
}

// Create a safe search result card.
function createResultCard(result, index) {
    const card = document.createElement("article");
    card.className = "result-card";

    const title = document.createElement("a");
    title.className = "result-title";
    title.textContent = result.title || "Untitled document";

    const url = document.createElement("span");
    url.className = "result-url";
    url.textContent = result.url || "URL unavailable";

    // Only allow safe HTTP and HTTPS document links.
    if (result.url) {
        try {
            const parsedUrl = new URL(result.url);

            if (["http:", "https:"].includes(parsedUrl.protocol)) {
                title.href = parsedUrl.href;
                title.target = "_blank";
                title.rel = "noopener noreferrer";
            }
        } catch (error) {
            // Display the title without a clickable link.
        }
    }

    const details = document.createElement("div");
    details.className = "result-details";

    const rank = document.createElement("span");
    rank.className = "rank-badge";
    rank.textContent = `Rank #${index + 1}`;

    const documentId = document.createElement("span");
    documentId.textContent = `ID: ${result.doc_id}`;

    const score = document.createElement("span");
    score.className = "score";

    const numericScore = Number(result.score);
    score.textContent = Number.isFinite(numericScore)
        ? `Score: ${numericScore.toFixed(4)}`
        : "Score unavailable";

    details.append(rank, documentId, score);
    card.append(title, url, details);

    return card;
}

// Display the returned search results.
function displayResults(data) {
    resultsContainer.replaceChildren();

    resultsSection.classList.remove("hidden");
    welcomeMessage.classList.add("hidden");

    resultCount.textContent =
        `${data.total_results} results · Version ${data.version}`;

    if (!data.results || data.results.length === 0) {
        const empty = document.createElement("div");
        empty.className = "empty-results";

        const heading = document.createElement("h3");
        heading.textContent = "No results found";

        const message = document.createElement("p");
        message.textContent =
            "Try another keyword or a different search phrase.";

        empty.append(heading, message);
        resultsContainer.appendChild(empty);
        return;
    }

    data.results.forEach((result, index) => {
        const card = createResultCard(result, index);
        resultsContainer.appendChild(card);
    });
}

// Perform a search through the FastAPI backend.
async function performSearch(query) {
    setLoading(true);

    errorMessage.classList.add("hidden");
    resultsSection.classList.add("hidden");
    welcomeMessage.classList.add("hidden");

    try {
        const params = new URLSearchParams({
            q: query,
            limit: 10
        });

        if (!versionSelect.disabled && versionSelect.value !== "") {
            params.set("version", versionSelect.value);
        }

        const response = await fetch(
            `${API_BASE}/search?${params.toString()}`
        );

        const data = await response.json();

        if (!response.ok) {
            throw new Error(
                data.detail || "Unable to complete your search."
            );
        }

        displayResults(data);

    } catch (error) {
        showError(
            error.message || "Something went wrong. Please try again."
        );
    } finally {
        setLoading(false);
    }
}

// Handle form submission.
searchForm.addEventListener("submit", (event) => {
    event.preventDefault();

    const query = searchInput.value.trim();

    if (!query) {
        showError("Please enter a search query.");
        return;
    }

    performSearch(query);
});

// Check backend availability and discover index versions.
checkHealth();
loadVersions();
