import { For, Show, createEffect, createMemo, createSignal, onCleanup, onMount } from "solid-js";

const CROSSFADE_MS = 900;
const clampUnit = (value) => Math.max(0, Math.min(1, value));

const formatTime = (seconds) => {
  if (!Number.isFinite(seconds) || seconds < 0) {
    return "0:00";
  }
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60)
    .toString()
    .padStart(2, "0");
  return `${mins}:${secs}`;
};

const IconButton = (props) => (
  <button
    type="button"
    onClick={props.onClick}
    aria-label={props.label}
    title={props.label}
    class={`transition-colors ${props.active ? "text-[var(--fg)]" : "text-[var(--soft)] hover:text-[var(--fg)]"} ${props.class || ""}`}
  >
    {props.children}
  </button>
);

const PlayIcon = () => (
  <svg viewBox="0 0 24 24" class="h-4 w-4 fill-current">
    <path d="M8 5v14l11-7z" />
  </svg>
);

const PauseIcon = () => (
  <svg viewBox="0 0 24 24" class="h-4 w-4 fill-current">
    <path d="M7 5h4v14H7zM13 5h4v14h-4z" />
  </svg>
);

const PrevIcon = () => (
  <svg viewBox="0 0 24 24" class="h-[18px] w-[18px] fill-current">
    <path d="M6 6h2v12H6zM18 6v12l-8-6z" />
  </svg>
);

const NextIcon = () => (
  <svg viewBox="0 0 24 24" class="h-[18px] w-[18px] fill-current">
    <path d="M16 6h2v12h-2zM6 6l8 6-8 6z" />
  </svg>
);

const ShuffleIcon = () => (
  <svg viewBox="0 0 24 24" class="h-4 w-4 fill-none stroke-current stroke-2">
    <path d="M16 3h5v5" />
    <path d="M4 20 21 3" />
    <path d="M21 16v5h-5" />
    <path d="m15 15 6 6" />
    <path d="M4 4l5 5" />
  </svg>
);

const RepeatIcon = () => (
  <svg viewBox="0 0 24 24" class="h-4 w-4 fill-none stroke-current stroke-2">
    <path d="M17 1l4 4-4 4" />
    <path d="M3 11V9a4 4 0 0 1 4-4h14" />
    <path d="M7 23l-4-4 4-4" />
    <path d="M21 13v2a4 4 0 0 1-4 4H3" />
  </svg>
);

const RepeatOneIcon = () => (
  <span class="relative inline-flex h-4 w-4 items-center justify-center">
    <RepeatIcon />
    <span class="absolute -bottom-1.5 -right-1.5 flex h-3.5 min-w-3.5 items-center justify-center rounded-full border border-current bg-[var(--bg)] px-[2px] font-mono text-[8px] leading-none">
      1
    </span>
  </span>
);

const RepeatAlbumIcon = () => (
  <span class="relative inline-flex h-4 w-4 items-center justify-center">
    <RepeatIcon />
    <span class="absolute -bottom-1.5 -right-1.5 flex h-3.5 min-w-3.5 items-center justify-center rounded-full border border-current bg-[var(--bg)] px-[2px] font-mono text-[7px] uppercase leading-none">
      A
    </span>
  </span>
);

const VolumeIcon = (props) => (
  <svg viewBox="0 0 24 24" class="h-[14px] w-[14px] fill-none stroke-current stroke-2">
    <path d="M11 5 6 9H3v6h3l5 4z" />
    <Show when={!props.muted}>
      <path d="M15.5 8.5a5 5 0 0 1 0 7" />
      <path d="M18.5 5.5a9 9 0 0 1 0 13" />
    </Show>
    <Show when={props.muted}>
      <path d="m16 9 5 5" />
      <path d="m21 9-5 5" />
    </Show>
  </svg>
);

const HeartIcon = (props) => (
  <svg viewBox="0 0 24 24" class={`h-[14px] w-[14px] ${props.filled ? "fill-current stroke-current" : "fill-none stroke-current"} stroke-2`}>
    <path d="M12 21s-6.7-4.35-9.2-8.13C.56 9.57 2.07 5 6.16 5c2.18 0 3.52 1.2 4.18 2.32C11 6.2 12.34 5 14.52 5c4.1 0 5.61 4.57 3.36 7.87C18.7 14.5 12 21 12 21z" />
  </svg>
);

const PlayingBars = () => (
  <span class="inline-flex h-3 items-end gap-px">
    <span class="playing-bar h-2 w-px bg-current" />
    <span class="playing-bar playing-bar-delay-1 h-3 w-px bg-current" />
    <span class="playing-bar playing-bar-delay-2 h-[6px] w-px bg-current" />
  </span>
);

const BrandIcon = () => (
  <span class="flex h-6 w-6 items-end justify-center border border-current p-1">
    <span class="flex items-end gap-px text-current">
      <span class="h-2 w-px bg-current" />
      <span class="h-4 w-px bg-current" />
      <span class="h-3 w-px bg-current" />
    </span>
  </span>
);

const UserIcon = () => (
  <svg viewBox="0 0 24 24" class="h-[14px] w-[14px] fill-none stroke-current stroke-[1.8]">
    <path d="M12 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8Z" />
    <path d="M4 20a8 8 0 0 1 16 0" />
  </svg>
);

const LogoutIcon = () => (
  <svg viewBox="0 0 24 24" class="h-[14px] w-[14px] fill-none stroke-current stroke-[1.8]">
    <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
    <path d="M16 17l5-5-5-5" />
    <path d="M21 12H9" />
  </svg>
);

const SpotifyIcon = () => (
  <svg viewBox="0 0 24 24" class="h-[14px] w-[14px] fill-none stroke-current stroke-[1.7]">
    <circle cx="12" cy="12" r="9" />
    <path d="M8 10.5c2.7-1 5.8-.8 8.6.6" />
    <path d="M8.8 13.3c2-.7 4.2-.5 6 .5" />
    <path d="M9.8 15.8c1.2-.4 2.5-.3 3.6.3" />
  </svg>
);

const ThemeIcon = (props) => (
  <svg viewBox="0 0 24 24" class="h-[14px] w-[14px] fill-none stroke-current stroke-[1.8]">
    <Show
      when={props.theme === "dark"}
      fallback={
        <>
          <path d="M12 3v2.5" />
          <path d="M12 18.5V21" />
          <path d="M4.9 4.9 6.7 6.7" />
          <path d="m17.3 17.3 1.8 1.8" />
          <path d="M3 12h2.5" />
          <path d="M18.5 12H21" />
          <path d="m4.9 19.1 1.8-1.8" />
          <path d="m17.3 6.7 1.8-1.8" />
          <circle cx="12" cy="12" r="4" />
        </>
      }
    >
      <path d="M20 14.5A7.5 7.5 0 1 1 9.5 4 6 6 0 0 0 20 14.5Z" />
    </Show>
  </svg>
);

const getInitials = (name, email = "") => {
  const parts = (name || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (parts.length >= 2) {
    return `${parts[0][0] || ""}${parts[parts.length - 1][0] || ""}`.toUpperCase();
  }
  if (parts.length === 1 && parts[0]) {
    return parts[0].slice(0, 2).toUpperCase();
  }
  const local = (email || "").split("@")[0].replace(/[^a-z0-9]/gi, "");
  return (local.slice(0, 2) || "U").toUpperCase();
};

const base64UrlEncode = (buffer) =>
  btoa(String.fromCharCode(...new Uint8Array(buffer)))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");

const createPkceVerifier = () => {
  const bytes = new Uint8Array(64);
  crypto.getRandomValues(bytes);
  return base64UrlEncode(bytes);
};

const shuffleArray = (items) => {
  const next = [...items];
  for (let index = next.length - 1; index > 0; index -= 1) {
    const swapIndex = Math.floor(Math.random() * (index + 1));
    [next[index], next[swapIndex]] = [next[swapIndex], next[index]];
  }
  return next;
};

const defaultUserPreferences = () => ({
  themePreference: "system",
  mainTab: "library",
  recentSongIds: [],
  playerVolume: 0.9,
  playerMuted: false,
  repeatMode: "off",
  autoplayNext: true,
});

const GUEST_PREFERENCES_KEY = "isaibox-guest-preferences";

const readGuestPreferences = () => {
  try {
    const raw = localStorage.getItem(GUEST_PREFERENCES_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
};

const writeGuestPreferences = (preferences) => {
  try {
    localStorage.setItem(GUEST_PREFERENCES_KEY, JSON.stringify(preferences));
  } catch {}
};

const clearGuestPreferences = () => {
  try {
    localStorage.removeItem(GUEST_PREFERENCES_KEY);
  } catch {}
};

function App() {
  const [songs, setSongs] = createSignal([]);
  const [results, setResults] = createSignal({ songs: [], albums: [], artists: [] });
  const [stats, setStats] = createSignal(null);
  const [query, setQuery] = createSignal("");
  const [selectedId, setSelectedId] = createSignal("");
  const [currentTrackId, setCurrentTrackId] = createSignal("");
  const [loading, setLoading] = createSignal(true);
  const [error, setError] = createSignal("");
  const [pendingQueryId, setPendingQueryId] = createSignal(0);
  const [isPlaying, setIsPlaying] = createSignal(false);
  const [currentTime, setCurrentTime] = createSignal(0);
  const [duration, setDuration] = createSignal(0);
  const [volume, setVolume] = createSignal(0.9);
  const [muted, setMuted] = createSignal(false);
  const [repeatMode, setRepeatMode] = createSignal("off");
  const [movieFilter, setMovieFilter] = createSignal("");
  const [artistFilter, setArtistFilter] = createSignal("");
  const [autoplayNext, setAutoplayNext] = createSignal(true);
  const [themePreference, setThemePreference] = createSignal("system");
  const [systemTheme, setSystemTheme] = createSignal("dark");
  const [mainTab, setMainTab] = createSignal("library");
  const [recentIds, setRecentIds] = createSignal([]);
  const [radioQueue, setRadioQueue] = createSignal([]);
  const [radioStations, setRadioStations] = createSignal([]);
  const [selectedRadioStationId, setSelectedRadioStationId] = createSignal("");
  const [radioLoading, setRadioLoading] = createSignal(false);
  const [radioMessage, setRadioMessage] = createSignal("");
  const [streamStarted, setStreamStarted] = createSignal(false);
  const [keyboardNavigating, setKeyboardNavigating] = createSignal(false);
  const [googleClientId, setGoogleClientId] = createSignal("");
  const [geminiRadioEnabled, setGeminiRadioEnabled] = createSignal(false);
  const [geminiKeyCount, setGeminiKeyCount] = createSignal(0);
  const [spotifyClientId, setSpotifyClientId] = createSignal("");
  const [spotifyRedirectUri, setSpotifyRedirectUri] = createSignal("");
  const [spotifyScopes, setSpotifyScopes] = createSignal("");
  const [user, setUser] = createSignal(null);
  const [favoriteIds, setFavoriteIds] = createSignal([]);
  const [playlists, setPlaylists] = createSignal([]);
  const [globalPlaylists, setGlobalPlaylists] = createSignal([]);
  const [playlistNameInput, setPlaylistNameInput] = createSignal("");
  const [globalPlaylistNameInput, setGlobalPlaylistNameInput] = createSignal("");
  const [spotifyImportUrl, setSpotifyImportUrl] = createSignal("");
  const [accountMessage, setAccountMessage] = createSignal("");
  const [selectedPlaylistTarget, setSelectedPlaylistTarget] = createSignal("");
  const [selectedGlobalPlaylistTarget, setSelectedGlobalPlaylistTarget] = createSignal("");
  const [adminOpen, setAdminOpen] = createSignal(false);
  const [adminUsers, setAdminUsers] = createSignal([]);
  const [airflowStatus, setAirflowStatus] = createSignal(null);
  const [adminMessage, setAdminMessage] = createSignal("");
  const [searchTab, setSearchTab] = createSignal("songs");
  const [globalPlaylistDetail, setGlobalPlaylistDetail] = createSignal(null);
  const [googleReady, setGoogleReady] = createSignal(false);
  const [googleInitialized, setGoogleInitialized] = createSignal(false);
  const [preferencesReady, setPreferencesReady] = createSignal(false);
  const [preferenceStore, setPreferenceStore] = createSignal("pending");
  const [spotifyAuth, setSpotifyAuth] = createSignal(null);
  const [spotifyPlaylists, setSpotifyPlaylists] = createSignal([]);
  const [selectedSpotifyPlaylistId, setSelectedSpotifyPlaylistId] = createSignal("");

  let worker;
  const audioRefs = [];
  let googleButtonRef;
  let listRef;
  let searchTimeout;
  let removeKeydownListener = null;
  let prefetchTimer;
  let keyboardNavTimer;
  let scrollAnimationFrame;
  let adminRefreshTimer;
  let crossfadeFrame;
  let themeMediaQuery;
  let syncSystemTheme;
  let crossfadeToken = 0;
  let activeDeckIndex = 0;
  let fadingAudio = null;
  const prefetchedIds = new Set();
  const rowRefs = new Map();

  const easeOutQuint = (value) => 1 - (1 - value) ** 5;

  const animateListScroll = (targetScrollTop) => {
    if (!listRef) {
      return;
    }

    cancelAnimationFrame(scrollAnimationFrame);
    const startTop = listRef.scrollTop;
    const delta = targetScrollTop - startTop;
    if (Math.abs(delta) < 2) {
      listRef.scrollTop = targetScrollTop;
      return;
    }

    const duration = 110;
    const start = performance.now();

    const step = (now) => {
      const elapsed = now - start;
      const progress = Math.min(1, elapsed / duration);
      listRef.scrollTop = startTop + delta * easeOutQuint(progress);
      if (progress < 1) {
        scrollAnimationFrame = requestAnimationFrame(step);
      }
    };

    scrollAnimationFrame = requestAnimationFrame(step);
  };

  const prefetchSongIds = (ids) => {
    const filteredIds = [...new Set(ids.filter(Boolean))]
      .filter((id) => !prefetchedIds.has(id))
      .slice(0, 4);
    if (!filteredIds.length) {
      return;
    }

    filteredIds.forEach((id) => prefetchedIds.add(id));
    clearTimeout(prefetchTimer);
    prefetchTimer = setTimeout(() => {
      void fetch("/api/prefetch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ids: filteredIds })
      }).catch(() => {
        filteredIds.forEach((id) => prefetchedIds.delete(id));
      });
    }, 120);
  };

  const visibleResults = createMemo(() => {
    const resultSongs = results().songs || [];
    const filteredByAlbum = movieFilter() ? resultSongs.filter((song) => song.movie === movieFilter()) : resultSongs;
    const filtered = artistFilter()
      ? filteredByAlbum.filter((song) => (song.singers || "").toLowerCase().includes(artistFilter().toLowerCase()))
      : filteredByAlbum;
    return filtered.slice(0, 200);
  });
  const visibleAlbums = createMemo(() => results().albums || []);
  const visibleArtists = createMemo(() => results().artists || []);
  const songIndex = createMemo(() => new Map(songs().map((song) => [song.id, song])));
  const recentSongs = createMemo(() => recentIds().map((id) => songIndex().get(id)).filter(Boolean));
  const favoriteSongs = createMemo(() => favoriteIds().map((id) => songIndex().get(id)).filter(Boolean));
  const theme = createMemo(() => (themePreference() === "system" ? systemTheme() : themePreference()));
  const currentRadioStation = createMemo(() => radioStations().find((station) => station.id === selectedRadioStationId()) || null);
  const activeSongList = createMemo(() => {
    if (mainTab() === "recents") {
      return recentSongs();
    }
    if (mainTab() === "favorites") {
      return favoriteSongs();
    }
    if (mainTab() === "radio") {
      return radioQueue();
    }
    return visibleResults();
  });
  const selectedSong = createMemo(() => {
    const visible = activeSongList();
    return visible.find((song) => song.id === selectedId()) || songs().find((song) => song.id === selectedId()) || null;
  });
  const currentSong = createMemo(() => {
    const list = songs();
    return list.find((song) => song.id === currentTrackId()) || null;
  });
  const selectedIndex = createMemo(() => activeSongList().findIndex((song) => song.id === selectedId()));
  const favoriteIdSet = createMemo(() => new Set(favoriteIds()));
  const formatUpdatedAt = (value) => {
    if (!value) return "Unknown";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "Unknown";
    return date.toLocaleString();
  };

  const spotifyConnected = createMemo(() => {
    const auth = spotifyAuth();
    return Boolean(auth?.accessToken && auth?.expiresAt && auth.expiresAt > Date.now());
  });

  const buildRadioQueue = (currentId = "") => {
    const pool = shuffleArray(songs().filter((song) => song.id !== currentId));
    const current = currentId ? songIndex().get(currentId) : null;
    return current ? [current, ...pool] : pool;
  };

  const applyUserPreferences = (payload = {}) => {
    const defaults = defaultUserPreferences();
    const next = {
      ...defaults,
      ...(payload || {}),
    };
    const nextThemePreference = ["system", "light", "dark"].includes(next.themePreference) ? next.themePreference : defaults.themePreference;
    const nextMainTab = ["library", "recents", "favorites", "radio"].includes(next.mainTab) ? next.mainTab : defaults.mainTab;
    const nextRepeatMode = ["off", "one", "album", "random"].includes(next.repeatMode) ? next.repeatMode : defaults.repeatMode;
    const nextRecentSongIds = Array.isArray(next.recentSongIds) ? next.recentSongIds.filter((id) => typeof id === "string" && id).slice(0, 80) : [];
    const nextPlayerVolume = Number.isFinite(Number(next.playerVolume)) ? clampUnit(Number(next.playerVolume)) : defaults.playerVolume;

    setThemePreference(nextThemePreference);
    setMainTab(nextMainTab);
    setRecentIds(nextRecentSongIds);
    setVolume(nextPlayerVolume);
    setMuted(Boolean(next.playerMuted));
    setRepeatMode(nextRepeatMode);
    setAutoplayNext(Boolean(next.autoplayNext));
  };

  const resetUserScopedPreferences = (payload = readGuestPreferences()) => {
    applyUserPreferences(payload || defaultUserPreferences());
    setPreferencesReady(true);
  };

  const collectCurrentPreferences = () => ({
    themePreference: themePreference(),
    mainTab: mainTab(),
    recentSongIds: recentIds(),
    playerVolume: volume(),
    playerMuted: muted(),
    repeatMode: repeatMode(),
    autoplayNext: autoplayNext(),
  });

  const mergePreferences = (base = {}, incoming = {}) => {
    const defaults = defaultUserPreferences();
    return {
      ...defaults,
      ...base,
      ...incoming,
      recentSongIds: [...new Set([...(incoming.recentSongIds || []), ...(base.recentSongIds || [])])].slice(0, 80),
    };
  };

  const toggleThemePreference = () => {
    setThemePreference(theme() === "dark" ? "light" : "dark");
  };

  const applyRadioStation = (stationId, autoplay = false) => {
    const station = radioStations().find((item) => item.id === stationId) || radioStations()[0];
    if (!station) {
      const queue = buildRadioQueue(currentTrackId());
      setRadioQueue(queue);
      if (queue[0]) {
        setSelectedId(queue[0].id);
        if (autoplay) {
          loadSong(queue[0], true);
        }
      }
      return;
    }
    const queue = (station.songIds || []).map((id) => songIndex().get(id)).filter(Boolean);
    setSelectedRadioStationId(station.id);
    setRadioQueue(queue);
    setMainTab("radio");
    if (queue[0]) {
      setSelectedId(queue[0].id);
      if (autoplay) {
        loadSong(queue[0], true);
      }
    }
  };

  const fetchRadioStations = async (forceRefresh = false) => {
    if (!songs().length) {
      return;
    }
    setRadioLoading(true);
    setRadioMessage("");
    try {
      const response = await fetch(`/api/radio/stations${forceRefresh ? "?refresh=1" : ""}`);
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.message || "Unable to build radio stations");
      }
      const stations = payload.stations || [];
      setRadioStations(stations);
      const nextStationId = stations.some((station) => station.id === selectedRadioStationId())
        ? selectedRadioStationId()
        : stations[0]?.id || "";
      setSelectedRadioStationId(nextStationId);
      setRadioMessage(payload.source === "gemini" ? "" : "Using local fallback station sorter");
      if (mainTab() === "radio" || !radioQueue().length) {
        applyRadioStation(nextStationId, false);
      }
    } catch (fetchError) {
      setRadioMessage(fetchError?.message || "Unable to build radio stations");
      if (!radioQueue().length) {
        setRadioQueue(buildRadioQueue(currentTrackId()));
      }
    } finally {
      setRadioLoading(false);
    }
  };

  const setMainBrowseTab = (tab) => {
    setMainTab(tab);
    if (tab === "radio" && !radioQueue().length && songs().length) {
      if (radioStations().length) {
        applyRadioStation(selectedRadioStationId() || radioStations()[0]?.id || "", false);
      } else {
        const queue = buildRadioQueue(currentTrackId());
        setRadioQueue(queue);
        if (!selectedId() && queue[0]) {
          setSelectedId(queue[0].id);
        }
      }
      return;
    }
    const list =
      tab === "recents" ? recentSongs()
      : tab === "favorites" ? favoriteSongs()
      : tab === "library" ? visibleResults()
      : activeSongList();
    if (list[0]) {
      setSelectedId(list[0].id);
    }
  };

  const rememberRecentSong = (songId) => {
    if (!songId) {
      return;
    }
    setRecentIds((current) => [songId, ...current.filter((id) => id !== songId)].slice(0, 80));
  };

  const clearRecents = () => {
    setRecentIds([]);
  };

  const startRadio = () => {
    applyRadioStation(selectedRadioStationId() || radioStations()[0]?.id || "", true);
  };

  const getAudio = (index) => audioRefs[index] || null;
  const getActiveAudio = () => getAudio(activeDeckIndex);
  const getInactiveAudio = () => getAudio(activeDeckIndex === 0 ? 1 : 0);

  const stopCrossfade = () => {
    crossfadeToken += 1;
    cancelAnimationFrame(crossfadeFrame);
    if (fadingAudio) {
      fadingAudio.pause();
      fadingAudio.currentTime = 0;
      fadingAudio.removeAttribute("src");
      fadingAudio.load();
      fadingAudio = null;
    }
  };

  const syncDeckVolumes = () => {
    audioRefs.forEach((audio, index) => {
      if (!audio) {
        return;
      }
      audio.muted = muted();
      audio.volume = index === activeDeckIndex ? volume() : 0;
    });
  };

  const syncTimelineFromAudio = (audio, resetCurrent = false) => {
    if (!audio) {
      if (resetCurrent) {
        setCurrentTime(0);
      }
      setDuration(0);
      return;
    }
    const nextDuration = Number.isFinite(audio.duration) && audio.duration > 0 ? audio.duration : 0;
    const nextCurrentTime = Number.isFinite(audio.currentTime) && audio.currentTime >= 0 ? audio.currentTime : 0;
    setDuration(nextDuration);
    setCurrentTime(resetCurrent ? 0 : Math.min(nextCurrentTime, nextDuration || nextCurrentTime));
  };

  const resetInactiveDeck = () => {
    const inactive = getInactiveAudio();
    if (!inactive) {
      return;
    }
    inactive.pause();
    inactive.currentTime = 0;
    inactive.removeAttribute("src");
    inactive.load();
    inactive.volume = 0;
  };

  const promoteDeck = (nextIndex) => {
    activeDeckIndex = nextIndex;
    syncDeckVolumes();
    syncTimelineFromAudio(getActiveAudio());
  };

  const beginCrossfade = (fromAudio, toAudio, nextDeckIndex) => {
    if (!fromAudio || !toAudio) {
      promoteDeck(nextDeckIndex);
      return;
    }

    stopCrossfade();
    fadingAudio = fromAudio;
    promoteDeck(nextDeckIndex);
    const fadeToken = crossfadeToken;
    const start = performance.now();
    fromAudio.volume = clampUnit(muted() ? 0 : volume());
    toAudio.volume = 0;

    const step = (now) => {
      if (fadeToken !== crossfadeToken) {
        return;
      }
      const progress = Math.min(1, (now - start) / CROSSFADE_MS);
      const target = clampUnit(muted() ? 0 : volume());
      fromAudio.volume = clampUnit(target * (1 - progress));
      toAudio.volume = clampUnit(target * progress);
      if (progress < 1) {
        crossfadeFrame = requestAnimationFrame(step);
        return;
      }
      if (fadingAudio === fromAudio) {
        fromAudio.pause();
        fromAudio.currentTime = 0;
        fromAudio.removeAttribute("src");
        fromAudio.load();
        fadingAudio = null;
      }
      syncDeckVolumes();
    };

    crossfadeFrame = requestAnimationFrame(step);
  };

  const refreshAccountState = async () => {
    try {
      setPreferenceStore("pending");
      setPreferencesReady(false);
      const sessionResponse = await fetch("/api/auth/session");
      const sessionPayload = await sessionResponse.json();
      const sessionUser = sessionPayload.user || null;
      setUser(sessionUser);

      if (!sessionUser) {
        setFavoriteIds([]);
        setPlaylists([]);
        setGlobalPlaylists([]);
        setSelectedPlaylistTarget("");
        setSelectedGlobalPlaylistTarget("");
        setAdminUsers([]);
        setAirflowStatus(null);
        setPreferenceStore("guest");
        resetUserScopedPreferences();
        return;
      }

      const [preferencesResponse, favoritesResponse, playlistsResponse] = await Promise.all([
        fetch("/api/me/preferences"),
        fetch("/api/favorites"),
        fetch("/api/playlists"),
      ]);

      let nextPreferences = defaultUserPreferences();
      if (preferencesResponse.ok) {
        const preferencesPayload = await preferencesResponse.json();
        nextPreferences = preferencesPayload.preferences || nextPreferences;
      }

      const guestPreferences = readGuestPreferences();
      if (guestPreferences) {
        const mergedPreferences = mergePreferences(nextPreferences, guestPreferences);
        const updateResponse = await fetch("/api/me/preferences", {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(mergedPreferences),
        });
        if (updateResponse.ok) {
          const updatePayload = await updateResponse.json();
          nextPreferences = updatePayload.preferences || mergedPreferences;
        } else {
          nextPreferences = mergedPreferences;
        }
        clearGuestPreferences();
      }
      applyUserPreferences(nextPreferences);
      setPreferenceStore("db");
      setPreferencesReady(true);

      if (!sessionUser.is_admin) {
        setAdminUsers([]);
        setAirflowStatus(null);
      }

      if (favoritesResponse.ok) {
        const favoritesPayload = await favoritesResponse.json();
        setFavoriteIds(favoritesPayload.songIds || []);
      } else {
        setFavoriteIds([]);
      }

      if (playlistsResponse.ok) {
        const playlistsPayload = await playlistsResponse.json();
        setPlaylists(playlistsPayload.playlists || []);
        setGlobalPlaylists(playlistsPayload.globalPlaylists || []);
        if (!selectedPlaylistTarget() && playlistsPayload.playlists?.[0]?.id) {
          setSelectedPlaylistTarget(playlistsPayload.playlists[0].id);
        }
        if (!selectedGlobalPlaylistTarget() && playlistsPayload.globalPlaylists?.[0]?.id) {
          setSelectedGlobalPlaylistTarget(playlistsPayload.globalPlaylists[0].id);
        }
      } else {
        setPlaylists([]);
        setGlobalPlaylists([]);
        setSelectedPlaylistTarget("");
        setSelectedGlobalPlaylistTarget("");
      }
    } catch {
      setUser(null);
      setFavoriteIds([]);
      setPlaylists([]);
      setGlobalPlaylists([]);
      setSelectedPlaylistTarget("");
      setSelectedGlobalPlaylistTarget("");
      setAdminUsers([]);
      setAirflowStatus(null);
      setPreferenceStore("guest");
      resetUserScopedPreferences();
    }
  };

  const refreshAdminState = async () => {
    if (!user()?.is_admin) {
      setAdminUsers([]);
      setAirflowStatus(null);
      return;
    }
    const response = await fetch("/api/admin/overview");
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.message || "Unable to load admin overview");
    }
    setAdminUsers(payload.users || []);
    setAirflowStatus(payload.airflow || null);
  };

  const handleGoogleCredential = async (credential) => {
    try {
      const response = await fetch("/api/auth/google", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ credential }),
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.message || "Google login failed");
      }
      setUser(payload.user || null);
      setAccountMessage("");
      await refreshAccountState();
    } catch (error) {
      setAccountMessage(error?.message || "Google login failed");
    }
  };

  const logout = async () => {
    await fetch("/api/auth/logout", { method: "POST" });
    setUser(null);
    setFavoriteIds([]);
    setPlaylists([]);
    setGlobalPlaylists([]);
    setSelectedPlaylistTarget("");
    setSelectedGlobalPlaylistTarget("");
    setAdminUsers([]);
    setAirflowStatus(null);
    setPreferenceStore("guest");
    resetUserScopedPreferences();
  };

  const beginGoogleLogin = () => {
    if (!googleReady() || !window.google?.accounts?.id) {
      setAccountMessage("Google sign-in unavailable");
      return;
    }
    const trigger = googleButtonRef?.querySelector?.('div[role="button"], iframe, [aria-labelledby]');
    if (trigger instanceof HTMLElement) {
      trigger.click();
      return;
    }
    setAccountMessage("");
    window.google.accounts.id.prompt();
  };

  const persistSpotifyAuth = (next) => {
    setSpotifyAuth(next);
    if (next) {
      localStorage.setItem("isaibox-spotify-auth", JSON.stringify(next));
    } else {
      localStorage.removeItem("isaibox-spotify-auth");
    }
  };

  const disconnectSpotify = () => {
    persistSpotifyAuth(null);
    setSpotifyPlaylists([]);
    setSelectedSpotifyPlaylistId("");
  };

  const refreshSpotifyToken = async () => {
    const auth = spotifyAuth();
    if (!auth?.refreshToken || !spotifyClientId()) {
      return auth?.accessToken || "";
    }
    const response = await fetch("https://accounts.spotify.com/api/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: spotifyClientId(),
        grant_type: "refresh_token",
        refresh_token: auth.refreshToken,
      }),
    });
    if (!response.ok) {
      disconnectSpotify();
      throw new Error("Spotify session expired");
    }
    const payload = await response.json();
    const nextAuth = {
      accessToken: payload.access_token,
      refreshToken: payload.refresh_token || auth.refreshToken,
      expiresAt: Date.now() + (payload.expires_in || 3600) * 1000,
      scope: payload.scope || auth.scope || "",
    };
    persistSpotifyAuth(nextAuth);
    return nextAuth.accessToken;
  };

  const ensureSpotifyAccessToken = async () => {
    const auth = spotifyAuth();
    if (!auth?.accessToken) {
      throw new Error("Connect Spotify first");
    }
    if (auth.expiresAt > Date.now() + 60_000) {
      return auth.accessToken;
    }
    return refreshSpotifyToken();
  };

  const beginSpotifyConnect = async () => {
    if (!spotifyClientId() || !spotifyRedirectUri()) {
      setAccountMessage("Spotify not configured");
      return;
    }
    const verifier = createPkceVerifier();
    const challengeBuffer = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
    const challenge = base64UrlEncode(challengeBuffer);
    const state = crypto.randomUUID();
    localStorage.setItem("isaibox-spotify-verifier", verifier);
    localStorage.setItem("isaibox-spotify-state", state);
    const authUrl = new URL("https://accounts.spotify.com/authorize");
    authUrl.search = new URLSearchParams({
      response_type: "code",
      client_id: spotifyClientId(),
      scope: spotifyScopes(),
      code_challenge_method: "S256",
      code_challenge: challenge,
      redirect_uri: spotifyRedirectUri(),
      state,
    }).toString();
    window.location.href = authUrl.toString();
  };

  const completeSpotifyAuthFromUrl = async () => {
    const url = new URL(window.location.href);
    const code = url.searchParams.get("code");
    const state = url.searchParams.get("state");
    const errorValue = url.searchParams.get("error");
    if (errorValue) {
      setAccountMessage("Spotify authorization failed");
      url.searchParams.delete("error");
      window.history.replaceState({}, "", url.toString());
      return;
    }
    if (!code) {
      const existing = localStorage.getItem("isaibox-spotify-auth");
      if (existing) {
        try {
          setSpotifyAuth(JSON.parse(existing));
        } catch {
          localStorage.removeItem("isaibox-spotify-auth");
        }
      }
      return;
    }

    const savedState = localStorage.getItem("isaibox-spotify-state");
    const verifier = localStorage.getItem("isaibox-spotify-verifier");
    if (!verifier || !savedState || savedState !== state) {
      setAccountMessage("Spotify authorization state mismatch");
      return;
    }

    const response = await fetch("https://accounts.spotify.com/api/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: spotifyClientId(),
        grant_type: "authorization_code",
        code,
        redirect_uri: spotifyRedirectUri(),
        code_verifier: verifier,
      }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error_description || "Spotify authorization failed");
    }
    persistSpotifyAuth({
      accessToken: payload.access_token,
      refreshToken: payload.refresh_token || "",
      expiresAt: Date.now() + (payload.expires_in || 3600) * 1000,
      scope: payload.scope || "",
    });
    localStorage.removeItem("isaibox-spotify-state");
    localStorage.removeItem("isaibox-spotify-verifier");
    url.searchParams.delete("code");
    url.searchParams.delete("state");
    window.history.replaceState({}, "", url.toString());
  };

  const fetchSpotifyPlaylists = async () => {
    const accessToken = await ensureSpotifyAccessToken();
    const response = await fetch("/api/spotify/playlists", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ accessToken }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.message || "Unable to load Spotify playlists");
    }
    setSpotifyPlaylists(payload.playlists || []);
    if (!selectedSpotifyPlaylistId() && payload.playlists?.[0]?.id) {
      setSelectedSpotifyPlaylistId(payload.playlists[0].id);
    }
  };

  const importSpotifyLikedSongs = async () => {
    if (!user()) {
      return;
    }
    setAccountMessage("Importing Spotify liked songs...");
    const accessToken = await ensureSpotifyAccessToken();
    const response = await fetch("/api/spotify/import/liked-songs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ accessToken }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.message || "Spotify liked songs import failed");
    }
    setAccountMessage(`Imported ${payload.matchedCount} of ${payload.totalCount} liked songs`);
    await refreshAccountState();
  };

  const importSpotifyAccountPlaylist = async () => {
    if (!user() || !selectedSpotifyPlaylistId()) {
      return;
    }
    setAccountMessage("Importing Spotify playlist...");
    const accessToken = await ensureSpotifyAccessToken();
    const response = await fetch("/api/spotify/import/playlist", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ accessToken, playlistId: selectedSpotifyPlaylistId() }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.message || "Spotify playlist import failed");
    }
    setAccountMessage(`Imported ${payload.matchedCount} of ${payload.totalCount} playlist tracks`);
    await refreshAccountState();
  };

  const toggleFavorite = async (songId) => {
    if (!user()) {
      setAccountMessage("Login required");
      return;
    }
    const liked = favoriteIdSet().has(songId);
    const response = await fetch(`/api/favorites/${songId}`, { method: liked ? "DELETE" : "POST" });
    if (response.ok) {
      setFavoriteIds((current) =>
        liked ? current.filter((id) => id !== songId) : [...current, songId]
      );
      return;
    }
    setAccountMessage("Unable to update favorites");
  };

  const createPlaylist = async () => {
    const name = playlistNameInput().trim();
    if (!name || !user()) {
      return;
    }
    const response = await fetch("/api/playlists", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    });
    const payload = await response.json();
    if (!response.ok) {
      setAccountMessage(payload.message || "Unable to create playlist");
      return;
    }
    setPlaylistNameInput("");
    setPlaylists((current) => [payload.playlist, ...current]);
    setSelectedPlaylistTarget(payload.playlist.id);
  };

  const createGlobalPlaylist = async () => {
    const name = globalPlaylistNameInput().trim();
    if (!name || !user()?.is_admin) {
      return;
    }
    const response = await fetch("/api/admin/playlists", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    });
    const payload = await response.json();
    if (!response.ok) {
      setAccountMessage(payload.message || "Unable to create global playlist");
      return;
    }
    setGlobalPlaylistNameInput("");
    setGlobalPlaylists((current) => [payload.playlist, ...current]);
    setSelectedGlobalPlaylistTarget(payload.playlist.id);
  };

  const addCurrentToPlaylist = async () => {
    if (!user() || !selectedPlaylistTarget() || !currentSong()) {
      return;
    }
    const response = await fetch(`/api/playlists/${selectedPlaylistTarget()}/songs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ songId: currentSong().id }),
    });
    if (!response.ok) {
      setAccountMessage("Unable to add song to playlist");
      return;
    }
    await refreshAccountState();
    setAccountMessage("Saved to playlist");
  };

  const addCurrentToGlobalPlaylist = async () => {
    if (!user()?.is_admin || !selectedGlobalPlaylistTarget() || !currentSong()) {
      return;
    }
    const response = await fetch(`/api/playlists/${selectedGlobalPlaylistTarget()}/songs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ songId: currentSong().id }),
    });
    if (!response.ok) {
      setAccountMessage("Unable to add song to global playlist");
      return;
    }
    await refreshAccountState();
    setAccountMessage("Saved to global playlist");
  };

  const importSpotifyPlaylist = async () => {
    if (!user() || !spotifyImportUrl().trim()) {
      return;
    }
    setAccountMessage("Importing Spotify playlist...");
    const response = await fetch("/api/playlists/import/spotify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: spotifyImportUrl().trim() }),
    });
    const payload = await response.json();
    if (!response.ok) {
      setAccountMessage(payload.message || "Spotify import failed");
      return;
    }
    setSpotifyImportUrl("");
    setAccountMessage(
      `${payload.updatedExisting ? "Updated" : "Imported"} playlist: matched ${payload.matchedCount} of ${payload.totalCount} tracks`
    );
    await refreshAccountState();
  };

  const generateAiPlaylists = async () => {
    if (!user()) {
      return;
    }
    setAccountMessage("Generating AI playlists...");
    const response = await fetch("/api/playlists/ai/generate", { method: "POST" });
    const payload = await response.json();
    if (!response.ok) {
      setAccountMessage(payload.message || "Unable to generate AI playlists");
      return;
    }
    await refreshAccountState();
    setAccountMessage(`Generated ${payload.playlists?.length || 0} AI playlists${payload.source === "gemini" ? "" : " with local fallback"}`);
  };

  const openGlobalPlaylist = async (playlistId) => {
    const response = await fetch(`/api/playlists/${playlistId}`);
    const payload = await response.json();
    if (!response.ok) {
      setAccountMessage(payload.message || "Unable to load global playlist");
      return;
    }
    setGlobalPlaylistDetail(payload.playlist);
  };

  const removeSongFromGlobalPlaylist = async (playlistId, songId) => {
    if (!user()?.is_admin) {
      return;
    }
    const response = await fetch(`/api/playlists/${playlistId}/songs/${songId}`, { method: "DELETE" });
    if (!response.ok) {
      setAccountMessage("Unable to remove song from global playlist");
      return;
    }
    await refreshAccountState();
    await openGlobalPlaylist(playlistId);
  };

  const runAdminAction = async (url, options = {}, successMessage = "") => {
    const response = await fetch(url, options);
    const payload = await response.json();
    if (!response.ok) {
      setAdminMessage(payload.message || payload.stderr || "Admin action failed");
      return;
    }
    setAdminMessage(successMessage || payload.stdout || "Admin action completed");
    await refreshAdminState();
  };

  const getSongNeighborhoodIds = (song) => {
    if (!song) {
      return [];
    }
    const list = activeSongList();
    const index = list.findIndex((item) => item.id === song.id);
    if (index < 0) {
      return [song.id];
    }
    return list.slice(index, index + 4).map((item) => item.id);
  };

  const cyclePlaybackMode = () => {
    const current = repeatMode();
    if (current === "off") {
      setRepeatMode("one");
      return;
    }
    if (current === "one") {
      setRepeatMode("album");
      return;
    }
    if (current === "album") {
      setRepeatMode("random");
      return;
    }
    setRepeatMode("off");
  };

  const playbackModeLabel = createMemo(() => {
    if (repeatMode() === "one") return "Single song loop";
    if (repeatMode() === "album") return "Album loop";
    if (repeatMode() === "random") return "Random";
    return "Normal";
  });

  const playbackModeIcon = createMemo(() => {
    if (repeatMode() === "one") {
      return <RepeatOneIcon />;
    }
    if (repeatMode() === "album") {
      return <RepeatAlbumIcon />;
    }
    if (repeatMode() === "random") {
      return <ShuffleIcon />;
    }
    return <RepeatIcon />;
  });

  const loadSong = (song, autoplay = false) => {
    const activeAudio = getActiveAudio();
    const inactiveAudio = getInactiveAudio();
    if (!song || !activeAudio || !inactiveAudio) {
      return;
    }

    const isSameSong = currentTrackId() === song.id;
    setSelectedId(song.id);
    setCurrentTrackId(song.id);
    rememberRecentSong(song.id);
    prefetchSongIds(getSongNeighborhoodIds(song));

    const version = encodeURIComponent(song.updatedAt || song.id);
    const nextRelativeUrl = `${song.audioUrl}?v=${version}`;
    const nextUrl = new URL(nextRelativeUrl, window.location.origin).href;
    if (activeAudio.src !== nextUrl) {
      stopCrossfade();
      setStreamStarted(false);
      inactiveAudio.pause();
      inactiveAudio.src = nextRelativeUrl;
      inactiveAudio.preload = "auto";
      inactiveAudio.currentTime = 0;
      inactiveAudio.load();
      syncTimelineFromAudio(inactiveAudio, true);
      if (autoplay) {
        inactiveAudio.volume = 0;
        inactiveAudio.muted = muted();
        void inactiveAudio.play()
          .then(() => {
            if (!activeAudio.src || activeAudio.paused) {
              if (activeAudio.src && activeAudio !== inactiveAudio) {
                activeAudio.pause();
                activeAudio.currentTime = 0;
                activeAudio.removeAttribute("src");
                activeAudio.load();
              }
              promoteDeck(activeDeckIndex === 0 ? 1 : 0);
              inactiveAudio.volume = muted() ? 0 : volume();
              setIsPlaying(true);
              setStreamStarted(true);
              syncTimelineFromAudio(inactiveAudio, true);
              return;
            }
            beginCrossfade(activeAudio, inactiveAudio, activeDeckIndex === 0 ? 1 : 0);
            setIsPlaying(true);
          })
          .catch(() => {
            inactiveAudio.pause();
          });
      } else if (!isSameSong) {
        promoteDeck(activeDeckIndex === 0 ? 1 : 0);
        activeAudio.pause();
      }
      return;
    }

    if (autoplay) {
      void activeAudio.play().catch(() => {});
    } else if (!isSameSong && activeAudio.preload !== "auto") {
      activeAudio.preload = "auto";
    }
  };

  const moveSelection = (offset) => {
    const nextSong = pickRelativeSong(offset);
    if (!nextSong) {
      return;
    }
    setSelectedId(nextSong.id);
  };

  const pickRelativeSong = (offset, baseId = selectedId()) => {
    const list = activeSongList();
    if (!list.length) {
      return null;
    }

    if (mainTab() === "radio") {
      const current = list.findIndex((song) => song.id === baseId);
      const nextIndex = current >= 0 ? (current + offset + list.length) % list.length : 0;
      return list[nextIndex] || null;
    }

    if (repeatMode() === "random") {
      const base = list.find((song) => song.id === baseId);
      const pool = list.filter((song) => song.id !== base?.id);
      return pool[Math.floor(Math.random() * pool.length)] || list[0];
    }

    const current = list.findIndex((song) => song.id === baseId);
    const from = current >= 0 ? current : 0;
    const next = Math.min(list.length - 1, Math.max(0, from + offset));
    return list[next];
  };

  const selectRelative = (offset, autoplay = false, baseId = selectedId()) => {
    const nextSong = pickRelativeSong(offset, baseId);
    if (!nextSong) {
      return;
    }
    if (autoplay) {
      loadSong(nextSong, true);
      return;
    }
    setSelectedId(nextSong.id);
  };

  const adjustSeek = (deltaSeconds) => {
    const activeAudio = getActiveAudio();
    if (!activeAudio || !currentSong()) {
      return;
    }
    const total = Number.isFinite(activeAudio.duration) && activeAudio.duration > 0 ? activeAudio.duration : duration();
    const next = Math.max(0, Math.min(total || 0, (activeAudio.currentTime || 0) + deltaSeconds));
    activeAudio.currentTime = next;
    setCurrentTime(next);
  };

  const adjustVolume = (delta) => {
    const next = Math.max(0, Math.min(1, volume() + delta));
    setMuted(false);
    setVolume(Number(next.toFixed(2)));
  };

  const togglePlayback = () => {
    const activeAudio = getActiveAudio();
    if (!activeAudio) {
      return;
    }

    if (!currentSong() && activeSongList()[0]) {
      loadSong(selectedSong() || activeSongList()[0], true);
      return;
    }

    if (activeAudio.paused) {
      void activeAudio.play().catch(() => {});
    } else {
      stopCrossfade();
      activeAudio.pause();
    }
  };

  const sendSearch = (value) => {
    if (!worker) {
      return;
    }
    const requestId = pendingQueryId() + 1;
    setPendingQueryId(requestId);
    worker.postMessage({ type: "search", payload: value, requestId });
  };

  onMount(async () => {
    applyUserPreferences(readGuestPreferences() || defaultUserPreferences());

    themeMediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
    syncSystemTheme = () => setSystemTheme(themeMediaQuery.matches ? "dark" : "light");
    syncSystemTheme();
    if (typeof themeMediaQuery.addEventListener === "function") {
      themeMediaQuery.addEventListener("change", syncSystemTheme);
    } else {
      themeMediaQuery.addListener(syncSystemTheme);
    }

    worker = new Worker(new URL("./search.worker.js", import.meta.url), { type: "module" });

    worker.onmessage = (event) => {
      if (event.data.type !== "results") {
        return;
      }
      if (event.data.requestId && event.data.requestId !== pendingQueryId()) {
        return;
      }
      setResults(event.data.payload);
      if (movieFilter() && !event.data.payload.songs.some((song) => song.movie === movieFilter())) {
        setMovieFilter("");
      }
      if (artistFilter() && !event.data.payload.songs.some((song) => (song.singers || "").toLowerCase().includes(artistFilter().toLowerCase()))) {
        setArtistFilter("");
      }
    };

    const onKeyDown = (event) => {
      const tagName = event.target?.tagName?.toLowerCase?.() || "";
      if (tagName === "input" || tagName === "textarea" || tagName === "range") {
        return;
      }

      const isModifierSeek = event.ctrlKey || event.metaKey;

      if (event.key === "ArrowDown") {
        event.preventDefault();
        if (isModifierSeek) {
          adjustVolume(-0.05);
          return;
        }
        setKeyboardNavigating(true);
        clearTimeout(keyboardNavTimer);
        keyboardNavTimer = setTimeout(() => setKeyboardNavigating(false), 180);
        selectRelative(1, false);
      } else if (event.key === "ArrowUp") {
        event.preventDefault();
        if (isModifierSeek) {
          adjustVolume(0.05);
          return;
        }
        setKeyboardNavigating(true);
        clearTimeout(keyboardNavTimer);
        keyboardNavTimer = setTimeout(() => setKeyboardNavigating(false), 180);
        selectRelative(-1, false);
      } else if (event.key === "ArrowLeft" && isModifierSeek) {
        event.preventDefault();
        adjustSeek(-5);
      } else if (event.key === "ArrowRight" && isModifierSeek) {
        event.preventDefault();
        adjustSeek(5);
      } else if (event.key === "Enter") {
        event.preventDefault();
        loadSong(selectedSong() || activeSongList()[0], true);
      } else if (event.key === " ") {
        event.preventDefault();
        if (!isPlaying()) {
          loadSong(selectedSong() || activeSongList()[0], true);
        } else {
          togglePlayback();
        }
      }
    };

    window.addEventListener("keydown", onKeyDown);
    removeKeydownListener = () => window.removeEventListener("keydown", onKeyDown);

    try {
      const [configResponse, statsResponse, songsResponse] = await Promise.all([
        fetch("/api/config"),
        fetch("/api/stats"),
        fetch("/api/library"),
      ]);

      if (!configResponse.ok || !statsResponse.ok || !songsResponse.ok) {
        throw new Error("Failed to load library");
      }

      const configPayload = await configResponse.json();
      const statsPayload = await statsResponse.json();
      const songsPayload = await songsResponse.json();
      const initialSongs = songsPayload.songs;

      setGoogleClientId(configPayload.googleClientId || "");
      setGeminiRadioEnabled(Boolean(configPayload.geminiRadioEnabled));
      setGeminiKeyCount(Number(configPayload.geminiKeyCount || 0));
      setSpotifyClientId(configPayload.spotifyClientId || "");
      setSpotifyRedirectUri(configPayload.spotifyRedirectUri || "");
      setSpotifyScopes(configPayload.spotifyScopes || "");
      setStats(statsPayload);
      setSongs(initialSongs);
      setResults({ songs: initialSongs.slice(0, 200), albums: [], artists: [] });
      setSelectedId(initialSongs[0]?.id || "");
      setCurrentTrackId(initialSongs[0]?.id || "");
      worker.postMessage({ type: "index", payload: initialSongs });
      prefetchSongIds(initialSongs.slice(0, 4).map((song) => song.id));

      if (initialSongs[0] && getActiveAudio()) {
        const version = encodeURIComponent(initialSongs[0].updatedAt || initialSongs[0].id);
        getActiveAudio().src = `${initialSongs[0].audioUrl}?v=${version}`;
        getActiveAudio().preload = "auto";
        syncDeckVolumes();
        syncTimelineFromAudio(getActiveAudio(), true);
      }

      await completeSpotifyAuthFromUrl();
      await refreshAccountState();
      await fetchRadioStations();
      if (user()?.is_admin) {
        await refreshAdminState();
      }
    } catch (err) {
      setError(err.message || "Failed to load");
    } finally {
      setLoading(false);
    }
  });

  createEffect(() => {
    clearTimeout(searchTimeout);
    const nextQuery = query().trim().toLowerCase();
    searchTimeout = setTimeout(() => sendSearch(nextQuery), 15);
  });

  createEffect(() => {
    syncDeckVolumes();
  });

  createEffect(() => {
    document.documentElement.dataset.theme = theme();
  });

  createEffect(() => {
    if (!preferencesReady() || preferenceStore() === "pending") {
      return;
    }
    if (preferenceStore() === "guest") {
      writeGuestPreferences(collectCurrentPreferences());
      return;
    }
    const payload = collectCurrentPreferences();
    void fetch("/api/me/preferences", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }).catch(() => {});
  });

  createEffect(() => {
    if (user()?.is_admin && adminOpen()) {
      void refreshAdminState().catch((error) => setAdminMessage(error?.message || "Unable to load admin panel"));
    }
  });

  createEffect(() => {
    clearInterval(adminRefreshTimer);
    if (user()?.is_admin && adminOpen()) {
      adminRefreshTimer = setInterval(() => {
        void refreshAdminState().catch(() => {});
      }, 10000);
    }
  });

  createEffect(() => {
    if (!googleClientId() || user() || !window.google?.accounts?.id || googleInitialized()) {
      return;
    }
    window.google.accounts.id.initialize({
      client_id: googleClientId(),
      callback: (response) => {
        if (response?.credential) {
          void handleGoogleCredential(response.credential);
        }
      },
    });
    setGoogleInitialized(true);
    setGoogleReady(true);
  });

  createEffect(() => {
    if (!googleReady() || user() || !googleButtonRef || !window.google?.accounts?.id) {
      return;
    }
    googleButtonRef.innerHTML = "";
    window.google.accounts.id.renderButton(googleButtonRef, {
      type: "standard",
      theme: theme() === "dark" ? "filled_black" : "outline",
      size: "large",
      shape: "pill",
      text: "signin_with",
      logo_alignment: "left",
      width: 240,
    });
  });

  createEffect(() => {
    if (!user() || !spotifyConnected()) {
      return;
    }
    if (spotifyPlaylists().length) {
      return;
    }
    void fetchSpotifyPlaylists().catch((error) => {
      setAccountMessage(error?.message || "Unable to load Spotify playlists");
    });
  });

  createEffect(() => {
    const list = activeSongList();
    if (!list.length) {
      if (selectedId()) {
        setSelectedId("");
      }
      return;
    }
    if (!list.some((song) => song.id === selectedId())) {
      setSelectedId(list[0].id);
    }
  });

  createEffect(() => {
    const currentId = selectedId();
    if (!currentId || !listRef) {
      return;
    }
    const row = rowRefs.get(currentId);
    if (!row) {
      return;
    }
    const rowTop = row.offsetTop;
    const rowBottom = rowTop + row.offsetHeight;
    const viewportTop = listRef.scrollTop;
    const viewportBottom = viewportTop + listRef.clientHeight;
    const topPadding = 8;
    const bottomPadding = 8;

    if (rowTop < viewportTop + topPadding) {
      animateListScroll(Math.max(0, rowTop - topPadding));
      return;
    }

    if (rowBottom > viewportBottom - bottomPadding) {
      animateListScroll(rowBottom - listRef.clientHeight + bottomPadding);
    }
  });

  createEffect(() => {
    if (mainTab() !== "radio" || radioQueue().length || !songs().length) {
      return;
    }
    if (radioStations().length) {
      applyRadioStation(selectedRadioStationId() || radioStations()[0]?.id || "", false);
      return;
    }
    setRadioQueue(buildRadioQueue(currentTrackId()));
  });

  onCleanup(() => {
    clearTimeout(searchTimeout);
    clearTimeout(prefetchTimer);
    clearTimeout(keyboardNavTimer);
    clearInterval(adminRefreshTimer);
    stopCrossfade();
    cancelAnimationFrame(scrollAnimationFrame);
    removeKeydownListener?.();
    worker?.terminate();
    audioRefs.forEach((audio) => audio?.pause());
    if (themeMediaQuery && syncSystemTheme) {
      if (typeof themeMediaQuery.removeEventListener === "function") {
        themeMediaQuery.removeEventListener("change", syncSystemTheme);
      } else {
        themeMediaQuery.removeListener(syncSystemTheme);
      }
    }
  });

  return (
    <main class="flex h-screen flex-col overflow-hidden bg-[var(--bg)] text-[var(--fg)]">
      <header class="flex items-center justify-between border-b border-[var(--line)] px-6 py-4">
        <span class="flex items-center gap-3">
          <BrandIcon />
          <span class="font-mono text-[11px] uppercase tracking-[0.32em] text-[var(--brand)]">isaibox</span>
        </span>
        <div class="flex items-center gap-2 md:gap-3">
          <div
            ref={(el) => {
              googleButtonRef = el;
            }}
            aria-hidden="true"
            class="pointer-events-none absolute left-[-9999px] top-0 opacity-0"
          />
          <Show
            when={user()}
            fallback={
              <button
                type="button"
                onClick={beginGoogleLogin}
                aria-label="Sign in with Google"
                title="Sign in with Google"
                class="flex h-9 w-9 items-center justify-center rounded-full border border-[var(--line)] text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)] disabled:cursor-not-allowed disabled:opacity-40"
                disabled={!googleReady()}
              >
                <UserIcon />
              </button>
            }
          >
            {(account) => (
              <div class="flex items-center gap-2 md:gap-3">
                <Show when={account().is_admin}>
                  <button
                    type="button"
                    onClick={() => setAdminOpen((value) => !value)}
                    class="rounded-full border border-[var(--line)] px-3 py-2 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)]"
                  >
                    Admin
                  </button>
                </Show>
                <span
                  title={account().name || account().email || "Account"}
                  class="flex h-9 w-9 items-center justify-center rounded-full border border-[var(--line)] font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--soft)]"
                >
                  {getInitials(account().name, account().email)}
                </span>
                <button
                  type="button"
                  onClick={() => void logout()}
                  aria-label="Logout"
                  title="Logout"
                  class="flex h-9 w-9 items-center justify-center rounded-full border border-[var(--line)] text-[var(--muted)] transition hover:border-[var(--fg)] hover:text-[var(--fg)]"
                >
                  <LogoutIcon />
                </button>
              </div>
            )}
          </Show>
          <button
            type="button"
            onClick={toggleThemePreference}
            aria-label={theme() === "dark" ? "Switch to light mode" : "Switch to dark mode"}
            title={theme() === "dark" ? "Switch to light mode" : "Switch to dark mode"}
            class="flex h-9 w-9 items-center justify-center rounded-full border border-[var(--line)] text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)]"
          >
            <ThemeIcon theme={theme()} />
          </button>
          <span class="hidden font-mono text-xs text-[var(--muted)] md:block">
            <Show when={stats()} fallback={"..."}>
              {stats().songs.toLocaleString()} tracks
            </Show>
          </span>
        </div>
      </header>

      <section class="border-b border-[var(--line)] px-6 py-3">
        <div class="flex flex-wrap items-center gap-2 font-mono text-[10px] uppercase tracking-[0.22em]">
          <button type="button" onClick={() => setMainBrowseTab("library")} class={mainTab() === "library" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>
            Library
          </button>
          <button type="button" onClick={() => setMainBrowseTab("recents")} class={mainTab() === "recents" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>
            Recents {recentSongs().length ? `(${recentSongs().length})` : ""}
          </button>
          <button type="button" onClick={() => setMainBrowseTab("favorites")} class={mainTab() === "favorites" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>
            Favorites {favoriteSongs().length ? `(${favoriteSongs().length})` : ""}
          </button>
          <button type="button" onClick={() => setMainBrowseTab("radio")} class={mainTab() === "radio" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>
            Radio
          </button>
        </div>
      </section>

      <Show when={mainTab() === "library"}>
        <section class="flex items-center gap-3 border-b border-[var(--line)] px-6 py-4">
          <span class="font-mono text-sm text-[var(--soft)]">/</span>
          <input
            value={query()}
            onInput={(event) => {
              setMovieFilter("");
              setArtistFilter("");
              setQuery(event.currentTarget.value);
            }}
            placeholder="Search tracks, singers, albums..."
            class="w-full bg-transparent font-mono text-sm tracking-wide text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
          />
          <button
            type="button"
            onClick={() => {
              setMovieFilter("");
              setArtistFilter("");
              setQuery("");
            }}
            class="font-mono text-xs uppercase tracking-[0.2em] text-[var(--muted)] transition hover:text-[var(--fg)]"
          >
            Clear
          </button>
        </section>
      </Show>

      <Show when={user()}>
        <section class="border-b border-[var(--line-soft)] px-6 py-3">
          <div class="flex flex-wrap items-center gap-3">
            <span class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">
              Favorites {favoriteIds().length}
            </span>
            <input
              value={playlistNameInput()}
              onInput={(event) => setPlaylistNameInput(event.currentTarget.value)}
              placeholder="New playlist"
              class="min-w-[140px] bg-transparent font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
            />
            <button
              type="button"
              onClick={() => void createPlaylist()}
              class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
            >
              Create playlist
            </button>
            <Show when={geminiRadioEnabled()}>
              <button
                type="button"
                onClick={() => void generateAiPlaylists()}
                class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                title={geminiKeyCount() ? `${geminiKeyCount()} Gemini keys configured` : "Gemini not configured"}
              >
                AI playlists
              </button>
            </Show>
            <Show when={playlists().length > 0}>
              <select
                value={selectedPlaylistTarget()}
                onChange={(event) => setSelectedPlaylistTarget(event.currentTarget.value)}
                class="bg-transparent font-mono text-xs text-[var(--fg)] outline-none"
              >
                <For each={playlists()}>
                  {(playlist) => <option value={playlist.id}>{playlist.name} ({playlist.trackCount})</option>}
                </For>
              </select>
              <button
                type="button"
                onClick={() => void addCurrentToPlaylist()}
                class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
              >
                Add current
              </button>
            </Show>
            <input
              value={spotifyImportUrl()}
              onInput={(event) => setSpotifyImportUrl(event.currentTarget.value)}
              placeholder="Paste Spotify playlist link"
              class="min-w-[220px] flex-1 bg-transparent font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
            />
            <button
              type="button"
              onClick={() => void importSpotifyPlaylist()}
              class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
            >
              Import Spotify
            </button>
            <Show when={spotifyClientId()}>
              <button
                type="button"
                onClick={() => void (spotifyConnected() ? disconnectSpotify() : beginSpotifyConnect())}
                title={spotifyConnected() ? "Disconnect Spotify" : "Connect Spotify"}
                class={`flex h-8 w-8 items-center justify-center rounded-full border transition ${
                  spotifyConnected()
                    ? "border-[var(--fg)] text-[var(--fg)]"
                    : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"
                }`}
              >
                <SpotifyIcon />
              </button>
              <Show when={spotifyConnected()}>
                <button
                  type="button"
                  onClick={() => void importSpotifyLikedSongs().catch((error) => setAccountMessage(error?.message || "Spotify liked songs import failed"))}
                  class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                >
                  Import likes
                </button>
                <Show when={spotifyPlaylists().length > 0}>
                  <select
                    value={selectedSpotifyPlaylistId()}
                    onChange={(event) => setSelectedSpotifyPlaylistId(event.currentTarget.value)}
                    class="bg-transparent font-mono text-xs text-[var(--fg)] outline-none"
                  >
                    <For each={spotifyPlaylists()}>
                      {(playlist) => <option value={playlist.id}>{playlist.name} ({playlist.trackCount})</option>}
                    </For>
                  </select>
                  <button
                    type="button"
                    onClick={() => void importSpotifyAccountPlaylist().catch((error) => setAccountMessage(error?.message || "Spotify playlist import failed"))}
                    class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                  >
                    Import selected
                  </button>
                </Show>
              </Show>
            </Show>
            <Show when={user()?.is_admin}>
              <input
                value={globalPlaylistNameInput()}
                onInput={(event) => setGlobalPlaylistNameInput(event.currentTarget.value)}
                placeholder="New global playlist"
                class="min-w-[160px] bg-transparent font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
              />
              <button
                type="button"
                onClick={() => void createGlobalPlaylist()}
                class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
              >
                Create global
              </button>
              <Show when={globalPlaylists().length > 0}>
                <select
                  value={selectedGlobalPlaylistTarget()}
                  onChange={(event) => setSelectedGlobalPlaylistTarget(event.currentTarget.value)}
                  class="bg-transparent font-mono text-xs text-[var(--fg)] outline-none"
                >
                  <For each={globalPlaylists()}>
                    {(playlist) => <option value={playlist.id}>{playlist.name} ({playlist.trackCount})</option>}
                  </For>
                </select>
                <button
                  type="button"
                  onClick={() => void addCurrentToGlobalPlaylist()}
                  class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                >
                  Add to global
                </button>
              </Show>
            </Show>
          </div>
          <Show when={accountMessage()}>
            <div class="mt-2 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)]">{accountMessage()}</div>
          </Show>
        </section>
      </Show>

      <Show when={globalPlaylists().length > 0}>
        <section class="border-b border-[var(--line-soft)] px-6 py-3">
          <div class="mb-2 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Global playlists</div>
          <div class="flex flex-wrap gap-2">
            <For each={globalPlaylists()}>
              {(playlist) => (
                <button
                  type="button"
                  onClick={() => void openGlobalPlaylist(playlist.id)}
                  class="border border-[var(--line)] px-3 py-2 text-left transition hover:border-[var(--fg)]"
                >
                  <div class="text-sm">{playlist.name}</div>
                  <div class="font-mono text-[10px] text-[var(--soft)]">{playlist.trackCount} tracks</div>
                </button>
              )}
            </For>
          </div>
          <Show when={globalPlaylistDetail()}>
            {(playlist) => (
              <div class="mt-4 border border-[var(--line)] p-3">
                <div class="mb-2 text-sm">{playlist().name}</div>
                <div class="space-y-2">
                  <For each={playlist().tracks || []}>
                    {(track, index) => (
                      <div class="flex items-center justify-between gap-4 border-b border-[var(--line-soft)] pb-2 text-sm last:border-b-0 last:pb-0">
                        <button type="button" onClick={() => loadSong(track, true)} class="min-w-0 text-left">
                          <span class="font-mono text-[10px] text-[var(--soft)]">{String(index() + 1).padStart(2, "0")}</span>
                          <span class="ml-3 truncate">{track.track}</span>
                        </button>
                        <Show when={user()?.is_admin}>
                          <button
                            type="button"
                            onClick={() => void removeSongFromGlobalPlaylist(playlist().id, track.id)}
                            class="font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                          >
                            Remove
                          </button>
                        </Show>
                      </div>
                    )}
                  </For>
                </div>
              </div>
            )}
          </Show>
        </section>
      </Show>

      <Show when={user()?.is_admin && adminOpen()}>
        <section class="border-b border-[var(--line-soft)] px-6 py-4">
          <div class="mb-3 flex flex-wrap items-center gap-3">
            <span class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Admin</span>
            <button
              type="button"
              onClick={() => void runAdminAction("/api/admin/airflow/trigger", { method: "POST" }, "Incremental scrape triggered")}
              class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
            >
              Trigger Incremental
            </button>
            <button
              type="button"
              onClick={() => void runAdminAction("/api/admin/airflow/start", { method: "POST" }, "Airflow start requested")}
              class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
            >
              Start Airflow
            </button>
            <button
              type="button"
              onClick={() => void runAdminAction("/api/admin/airflow/stop", { method: "POST" }, "Airflow stop requested")}
              class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
            >
              Stop Airflow
            </button>
            <button
              type="button"
              onClick={() => void runAdminAction("/api/admin/airflow/trigger-full", { method: "POST" }, "Full scrape triggered")}
              class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
            >
              Trigger Full Scrape
            </button>
            <button
              type="button"
              onClick={() => void refreshAdminState().catch((error) => setAdminMessage(error?.message || "Unable to refresh admin state"))}
              class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
            >
              Refresh
            </button>
          </div>

          <Show when={airflowStatus()}>
            {(status) => (
              <div class="mb-4 grid gap-2 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] md:grid-cols-4">
                <div>Webserver: {status().webserverRunning ? "Running" : "Stopped"}</div>
                <div>Latest run: {status().latestRun?.status || "unknown"}</div>
                <div>Songs total: {status().latestRun?.songsTotal || 0}</div>
                <div>DAG CLI: {status().dagsOk ? "Healthy" : "Error"}</div>
              </div>
            )}
          </Show>

          <div class="overflow-x-auto">
            <table class="w-full border-collapse text-left">
              <thead>
                <tr class="border-b border-[var(--line-soft)] font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">
                  <th class="py-2 pr-4">User</th>
                  <th class="py-2 pr-4">Role</th>
                  <th class="py-2 pr-4">Status</th>
                  <th class="py-2 pr-4">Last login</th>
                  <th class="py-2">Actions</th>
                </tr>
              </thead>
              <tbody>
                <For each={adminUsers()}>
                  {(adminUser) => (
                    <tr class="border-b border-[var(--line-soft)] text-sm">
                      <td class="py-3 pr-4">
                        <div>{adminUser.name || "-"}</div>
                        <div class="font-mono text-[11px] text-[var(--soft)]">{adminUser.email || adminUser.userId}</div>
                      </td>
                      <td class="py-3 pr-4 font-mono text-[11px] text-[var(--soft)]">
                        {adminUser.isAdmin ? "Admin" : "User"}
                      </td>
                      <td class="py-3 pr-4 font-mono text-[11px] text-[var(--soft)]">
                        {adminUser.isBanned ? `Banned${adminUser.banReason ? `: ${adminUser.banReason}` : ""}` : "Active"}
                      </td>
                      <td class="py-3 pr-4 font-mono text-[11px] text-[var(--soft)]">
                        {adminUser.lastLoginAt ? new Date(adminUser.lastLoginAt).toLocaleString() : "-"}
                      </td>
                      <td class="py-3">
                        <div class="flex flex-wrap gap-3 font-mono text-[10px] uppercase tracking-[0.22em]">
                          <button
                            type="button"
                            onClick={() =>
                              void runAdminAction(
                                `/api/admin/users/${adminUser.userId}/${adminUser.isBanned ? "unban" : "ban"}`,
                                {
                                  method: "POST",
                                  headers: { "Content-Type": "application/json" },
                                  body: JSON.stringify({ reason: "Banned by admin" }),
                                },
                                adminUser.isBanned ? "User unbanned" : "User banned"
                              )
                            }
                            class="text-[var(--soft)] transition hover:text-[var(--fg)]"
                          >
                            {adminUser.isBanned ? "Unban" : "Ban"}
                          </button>
                          <button
                            type="button"
                            onClick={() =>
                              void runAdminAction(
                                `/api/admin/users/${adminUser.userId}/admin`,
                                {
                                  method: "POST",
                                  headers: { "Content-Type": "application/json" },
                                  body: JSON.stringify({ isAdmin: !adminUser.isAdmin }),
                                },
                                adminUser.isAdmin ? "Admin removed" : "Admin granted"
                              )
                            }
                            class="text-[var(--soft)] transition hover:text-[var(--fg)]"
                          >
                            {adminUser.isAdmin ? "Remove admin" : "Make admin"}
                          </button>
                        </div>
                      </td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </div>

          <Show when={airflowStatus()?.recentRuns?.length}>
            <div class="mt-5 overflow-x-auto">
              <div class="mb-2 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Recent runs</div>
              <table class="w-full border-collapse text-left">
                <thead>
                  <tr class="border-b border-[var(--line-soft)] font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">
                    <th class="py-2 pr-4">Run</th>
                    <th class="py-2 pr-4">Status</th>
                    <th class="py-2 pr-4">Started</th>
                    <th class="py-2 pr-4">Finished</th>
                    <th class="py-2 pr-4">Pages</th>
                    <th class="py-2">Songs</th>
                  </tr>
                </thead>
                <tbody>
                  <For each={airflowStatus()?.recentRuns || []}>
                    {(run) => (
                      <tr class="border-b border-[var(--line-soft)] text-sm">
                        <td class="py-3 pr-4 font-mono text-[11px] text-[var(--soft)]">{run.runId}</td>
                        <td class="py-3 pr-4 font-mono text-[11px] text-[var(--soft)]">{run.status}</td>
                        <td class="py-3 pr-4 font-mono text-[11px] text-[var(--soft)]">{run.startedAt ? new Date(run.startedAt).toLocaleString() : "-"}</td>
                        <td class="py-3 pr-4 font-mono text-[11px] text-[var(--soft)]">{run.finishedAt ? new Date(run.finishedAt).toLocaleString() : "-"}</td>
                        <td class="py-3 pr-4 font-mono text-[11px] text-[var(--soft)]">{run.pagesScraped}</td>
                        <td class="py-3 font-mono text-[11px] text-[var(--soft)]">{run.songsTotal}</td>
                      </tr>
                    )}
                  </For>
                </tbody>
              </table>
            </div>
          </Show>

          <Show when={adminMessage()}>
            <div class="mt-3 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)]">{adminMessage()}</div>
          </Show>
        </section>
      </Show>

      <Show when={mainTab() === "library"}>
        <section class="border-b border-[var(--line-soft)] px-6 py-2">
          <div class="flex items-center gap-4 font-mono text-[10px] uppercase tracking-[0.22em]">
            <button type="button" onClick={() => setSearchTab("songs")} class={searchTab() === "songs" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>Songs</button>
            <button type="button" onClick={() => setSearchTab("albums")} class={searchTab() === "albums" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>Albums</button>
            <button type="button" onClick={() => setSearchTab("artists")} class={searchTab() === "artists" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>Artists</button>
          </div>
        </section>
      </Show>

      <Show when={mainTab() === "library" && (query().trim() || movieFilter() || artistFilter())}>
        <section class="border-b border-[var(--line-soft)] px-6 py-4">
          <div class="flex items-center justify-between gap-4">
            <div class="min-w-0">
              <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">
                {movieFilter() ? "Filtered by album" : artistFilter() ? "Filtered by artist" : "Search filters"}
              </div>
              <Show when={movieFilter()}>
                <div class="mt-1 truncate text-sm text-[var(--fg)]">{movieFilter()}</div>
              </Show>
              <Show when={!movieFilter() && artistFilter()}>
                <div class="mt-1 truncate text-sm text-[var(--fg)]">{artistFilter()}</div>
              </Show>
              <Show when={!movieFilter() && query().trim()}>
                <div class="mt-1 text-xs text-[var(--soft)]">Songs, albums, and artists are separate views over the same results.</div>
              </Show>
            </div>
            <Show when={movieFilter() || artistFilter()}>
              <button
                type="button"
                onClick={() => {
                  setMovieFilter("");
                  setArtistFilter("");
                  setSearchTab("songs");
                }}
                class="shrink-0 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--soft)] transition hover:text-[var(--fg)]"
              >
                Clear filter
              </button>
            </Show>
          </div>

          <Show when={!movieFilter() && !artistFilter() && searchTab() === "albums" && visibleAlbums().length > 0}>
            <div class="mt-3 flex flex-wrap gap-2">
              <For each={visibleAlbums()}>
                {(album) => (
                  <button
                    type="button"
                    onClick={() => {
                      setMovieFilter(album.album);
                      setSearchTab("songs");
                    }}
                    class="border border-[var(--line)] px-3 py-2 text-left text-[var(--fg)] transition hover:border-[var(--fg)]"
                    title={`${album.count} songs`}
                  >
                    <div class="text-sm">{album.album}</div>
                    <div class="font-mono text-[10px] text-[var(--soft)]">
                      {album.musicDirector || "-"} · {album.year || "-"} · {album.count}
                    </div>
                  </button>
                )}
              </For>
            </div>
          </Show>

          <Show when={!movieFilter() && !artistFilter() && searchTab() === "artists" && visibleArtists().length > 0}>
            <div class="mt-3 flex flex-wrap gap-2">
              <For each={visibleArtists()}>
                {(artist) => (
                  <button
                    type="button"
                    onClick={() => {
                      setArtistFilter(artist.artist);
                      setSearchTab("songs");
                    }}
                    class="border border-[var(--line)] px-3 py-2 text-left text-[var(--fg)] transition hover:border-[var(--fg)]"
                    title={`${artist.count} songs`}
                  >
                    <div class="text-sm">{artist.artist}</div>
                    <div class="font-mono text-[10px] text-[var(--soft)]">{artist.count} songs</div>
                  </button>
                )}
              </For>
            </div>
          </Show>

          <div class="mt-3 flex items-center justify-between font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">
            <span>{searchTab()}</span>
            <span>
              {searchTab() === "songs"
                ? visibleResults().length.toLocaleString()
                : searchTab() === "albums"
                  ? visibleAlbums().length.toLocaleString()
                  : visibleArtists().length.toLocaleString()} shown
            </span>
          </div>
        </section>
      </Show>

      <section class="flex min-h-0 flex-1 flex-col">
        <Show when={mainTab() !== "library"}>
          <section class="border-b border-[var(--line-soft)] px-6 py-4">
            <div class="flex flex-wrap items-center justify-between gap-3">
              <div>
                <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">
                  {mainTab() === "recents" ? "Recent plays" : mainTab() === "favorites" ? "Favorites" : "Radio station"}
                </div>
                <div class="mt-1 text-sm text-[var(--soft)]">
                  {mainTab() === "recents"
                    ? "Recently played tracks, kept like a rolling playlist."
                    : mainTab() === "favorites"
                      ? "Your liked tracks as a playable list."
                      : currentRadioStation()?.blurb || "Gemini-sorted stations with looping 100-song queues."}
                </div>
              </div>
              <div class="flex flex-wrap items-center gap-3 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)]">
                <span>{activeSongList().length} tracks</span>
                <Show when={mainTab() === "recents" && recentSongs().length > 0}>
                  <button type="button" onClick={clearRecents} class="transition hover:text-[var(--fg)]">Clear recents</button>
                </Show>
                <Show when={mainTab() === "radio"}>
                  <button type="button" onClick={startRadio} class="transition hover:text-[var(--fg)]">Start radio</button>
                  <button type="button" onClick={() => void fetchRadioStations(true)} class="transition hover:text-[var(--fg)]">
                    Refresh stations
                  </button>
                </Show>
              </div>
            </div>
            <Show when={mainTab() === "radio"}>
              <div class="mt-4">
                <div class="mb-2 flex flex-wrap items-center gap-3 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)]">
                  <span>{radioLoading() ? "Building stations..." : `${radioStations().length} stations`}</span>
                  <Show when={currentRadioStation()}>
                    <span>{currentRadioStation().yearStart} - {currentRadioStation().yearEnd}</span>
                  </Show>
                  <Show when={geminiRadioEnabled()}>
                    <span>Gemini radio</span>
                  </Show>
                  <Show when={radioMessage()}>
                    <span>{radioMessage()}</span>
                  </Show>
                </div>
                <div class="flex flex-wrap gap-2">
                  <For each={radioStations()}>
                    {(station) => (
                      <button
                        type="button"
                        onClick={() => applyRadioStation(station.id, false)}
                        class={`max-w-[220px] border px-3 py-2 text-left transition ${
                          selectedRadioStationId() === station.id
                            ? "border-[var(--fg)] bg-[var(--hover)]"
                            : "border-[var(--line)] hover:border-[var(--fg)]"
                        }`}
                      >
                        <div class="truncate text-sm">{station.name}</div>
                        <div class="mt-1 line-clamp-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]">
                          {station.blurb}
                        </div>
                        <div class="mt-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--muted)]">
                          {station.yearStart} - {station.yearEnd} · {station.trackCount}
                        </div>
                      </button>
                    )}
                  </For>
                </div>
              </div>
            </Show>
          </section>
        </Show>
        <Show when={mainTab() !== "library" || searchTab() === "songs" || movieFilter() || artistFilter()}>
          <>
            <div class="flex items-center gap-4 border-b border-[var(--line-soft)] px-6 py-2">
              <span class="w-8 text-right font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">#</span>
              <span class="min-w-0 flex-[1.2] font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Song</span>
              <span class="hidden min-w-0 flex-1 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] md:block">Singers</span>
              <span class="hidden min-w-0 flex-1 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] lg:block">Movie</span>
              <span class="hidden min-w-0 flex-1 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] xl:block">Music Director</span>
              <span class="w-20 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Year</span>
              <span class="w-8 text-right font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Fav</span>
            </div>

            <Show when={!loading()} fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--muted)]">Loading</div>}>
              <Show when={!error()} fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--soft)]">{error()}</div>}>
                <Show
                  when={activeSongList().length > 0}
                  fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--muted)]">No results</div>}
                >
                  <ul ref={listRef} class="min-h-0 flex-1 overflow-y-auto">
                    <For each={activeSongList()}>
                      {(song, index) => {
                        const active = () => selectedSong()?.id === song.id;
                        return (
                          <li>
                            <button
                              ref={(el) => {
                                if (el) {
                                  rowRefs.set(song.id, el);
                                } else {
                                  rowRefs.delete(song.id);
                                }
                              }}
                              type="button"
                              onClick={() => loadSong(song, true)}
                              title={`Last updated: ${formatUpdatedAt(song.updatedAt)}`}
                              class={`flex w-full items-center gap-4 border-b px-6 py-3 text-left transition ${
                                active()
                                  ? "song-row-active border-transparent text-[var(--fg)]"
                                  : "border-transparent bg-transparent text-[var(--fg)] hover:bg-[var(--hover)]"
                              } ${active() && keyboardNavigating() ? "bg-[var(--hover)]" : ""}`}
                            >
                              <span class="w-8 text-right font-mono text-xs text-[var(--soft)]">
                                {currentTrackId() === song.id && isPlaying() && streamStarted() ? <PlayingBars /> : String(index() + 1).padStart(2, "0")}
                              </span>
                              <span class="min-w-0 flex-[1.2] truncate text-sm">{song.track}</span>
                              <span class="hidden min-w-0 flex-1 truncate font-mono text-[11px] text-[var(--soft)] md:block">
                                {song.singers || "-"}
                              </span>
                              <span class="hidden min-w-0 flex-1 truncate font-mono text-[11px] text-[var(--soft)] lg:block">
                                {song.movie || "-"}
                              </span>
                              <span class="hidden min-w-0 flex-1 truncate font-mono text-[11px] text-[var(--soft)] xl:block">
                                {song.musicDirector || "-"}
                              </span>
                              <span class="w-20 font-mono text-[11px] text-[var(--soft)]">
                                {song.year || "-"}
                              </span>
                              <span class="flex w-8 justify-end">
                                <Show when={user()}>
                                  <span
                                    role="button"
                                    tabindex="-1"
                                    title={favoriteIdSet().has(song.id) ? "Remove favorite" : "Save favorite"}
                                    onClick={(event) => {
                                      event.stopPropagation();
                                      void toggleFavorite(song.id);
                                    }}
                                    class={`transition-colors ${favoriteIdSet().has(song.id) ? "text-[var(--fg)]" : "text-[var(--muted)] hover:text-[var(--fg)]"}`}
                                  >
                                    <HeartIcon filled={favoriteIdSet().has(song.id)} />
                                  </span>
                                </Show>
                              </span>
                            </button>
                          </li>
                        );
                      }}
                    </For>
                  </ul>
                </Show>
              </Show>
            </Show>
          </>
        </Show>
        <Show when={mainTab() === "library" && searchTab() === "albums" && !movieFilter() && !artistFilter()}>
          <div class="flex flex-1 items-start p-6">
            <div class="flex flex-wrap gap-2">
              <For each={visibleAlbums()}>
                {(album) => (
                  <button
                    type="button"
                    onClick={() => {
                      setMovieFilter(album.album);
                      setSearchTab("songs");
                    }}
                    class="border border-[var(--line)] px-3 py-2 text-left transition hover:border-[var(--fg)]"
                  >
                    <div class="text-sm">{album.album}</div>
                    <div class="font-mono text-[10px] text-[var(--soft)]">{album.count} songs</div>
                  </button>
                )}
              </For>
            </div>
          </div>
        </Show>
        <Show when={mainTab() === "library" && searchTab() === "artists" && !movieFilter() && !artistFilter()}>
          <div class="flex flex-1 items-start p-6">
            <div class="flex flex-wrap gap-2">
              <For each={visibleArtists()}>
                {(artist) => (
                  <button
                    type="button"
                    onClick={() => {
                      setArtistFilter(artist.artist);
                      setSearchTab("songs");
                    }}
                    class="border border-[var(--line)] px-3 py-2 text-left transition hover:border-[var(--fg)]"
                  >
                    <div class="text-sm">{artist.artist}</div>
                    <div class="font-mono text-[10px] text-[var(--soft)]">{artist.count} songs</div>
                  </button>
                )}
              </For>
            </div>
          </div>
        </Show>
      </section>

      <footer class="border-t border-[var(--line)] px-6 py-3">
        <div class="mb-2 flex justify-center">
          <div class="min-w-0 max-w-2xl text-center">
            <Show when={currentSong()} fallback={<p class="font-mono text-xs uppercase tracking-[0.25em] text-[var(--muted)]">No track selected</p>}>
              {(song) => (
                <p class="truncate text-[13px] font-semibold">{song().track}</p>
              )}
            </Show>
          </div>
        </div>

        <div class="mx-auto mb-2 flex w-full max-w-xl items-center gap-3">
          <span class="w-10 text-right font-mono text-[10px] text-[var(--muted)]">{formatTime(currentTime())}</span>
          <input
            type="range"
            min="0"
            max={duration() || 0}
            step="0.1"
            value={currentTime()}
            onInput={(event) => {
              const next = Number(event.currentTarget.value);
              const activeAudio = getActiveAudio();
              if (activeAudio) {
                activeAudio.currentTime = next;
              }
              setCurrentTime(next);
            }}
            class="flex-1"
            style={{ "accent-color": "var(--fg)" }}
          />
          <span class="w-10 font-mono text-[10px] text-[var(--muted)]">{formatTime(duration())}</span>
        </div>

        <div class="grid grid-cols-[1fr_auto_1fr] items-center gap-4">
          <div />
          <div class="flex items-center justify-center gap-5">
            <IconButton onClick={() => selectRelative(-1, true, currentTrackId() || selectedId())} label="Previous">
              <PrevIcon />
            </IconButton>
            <button
              type="button"
              onClick={togglePlayback}
              aria-label={isPlaying() ? "Pause" : "Play"}
              class="flex h-9 w-9 items-center justify-center border border-[var(--fg)] text-[var(--fg)] transition hover:bg-[var(--fg)] hover:text-[var(--bg)]"
            >
              {isPlaying() ? <PauseIcon /> : <PlayIcon />}
            </button>
            <IconButton onClick={() => selectRelative(1, true, currentTrackId() || selectedId())} label="Next">
              <NextIcon />
            </IconButton>
            <IconButton
              onClick={cyclePlaybackMode}
              active={repeatMode() !== "off"}
              label={`Playback mode: ${playbackModeLabel()}`}
              class="flex h-5 w-5 items-center justify-center"
            >
              {playbackModeIcon()}
            </IconButton>
          </div>
          <div class="flex items-center justify-end gap-2.5">
            <IconButton onClick={() => setMuted((value) => !value)} label={muted() ? "Unmute" : "Mute"}>
              <VolumeIcon muted={muted() || volume() === 0} />
            </IconButton>
            <input
              type="range"
              min="0"
              max="1"
              step="0.01"
              value={volume()}
              onInput={(event) => setVolume(Number(event.currentTarget.value))}
              class="w-20"
              style={{ "accent-color": "var(--fg)" }}
            />
            <span class="w-6 text-right font-mono text-[10px] text-[var(--muted)]">{Math.round(muted() ? 0 : volume() * 100)}</span>
          </div>
        </div>
        <For each={[0, 1]}>
          {(deckIndex) => (
            <audio
              ref={(el) => {
                audioRefs[deckIndex] = el;
              }}
              preload="auto"
              onPlay={(event) => {
                if (event.currentTarget === getActiveAudio()) {
                  setIsPlaying(true);
                  syncTimelineFromAudio(event.currentTarget);
                }
              }}
              onPlaying={(event) => {
                if (event.currentTarget === getActiveAudio()) {
                  setIsPlaying(true);
                  setStreamStarted(true);
                  syncTimelineFromAudio(event.currentTarget);
                }
              }}
              onWaiting={(event) => {
                if (event.currentTarget === getActiveAudio()) {
                  setStreamStarted(false);
                }
              }}
              onPause={(event) => {
                if (event.currentTarget === getActiveAudio()) {
                  setIsPlaying(false);
                }
              }}
              onLoadedMetadata={(event) => {
                if (event.currentTarget === getActiveAudio()) {
                  syncTimelineFromAudio(event.currentTarget);
                }
              }}
              onTimeUpdate={(event) => {
                if (event.currentTarget === getActiveAudio()) {
                  syncTimelineFromAudio(event.currentTarget);
                }
              }}
              onEnded={(event) => {
                if (event.currentTarget !== getActiveAudio()) {
                  return;
                }
                if (repeatMode() === "one") {
                  event.currentTarget.currentTime = 0;
                  void event.currentTarget.play().catch(() => {});
                  return;
                }
                if (!autoplayNext()) {
                  setIsPlaying(false);
                  return;
                }
                if (mainTab() === "radio") {
                  selectRelative(1, true, currentTrackId() || selectedId());
                  return;
                }
                if (repeatMode() === "album" || repeatMode() === "random") {
                  selectRelative(1, true, currentTrackId() || selectedId());
                  return;
                }
                const current = activeSongList().findIndex((song) => song.id === (currentTrackId() || selectedId()));
                if (current >= 0 && current < activeSongList().length - 1) {
                  selectRelative(1, true, currentTrackId() || selectedId());
                } else {
                  setIsPlaying(false);
                }
              }}
            />
          )}
        </For>
      </footer>
    </main>
  );
}

export default App;
