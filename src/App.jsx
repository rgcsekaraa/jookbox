import { For, Show, createEffect, createMemo, createSignal, onCleanup, onMount } from "solid-js";

const CROSSFADE_MS = 900;
const clampUnit = (value) => Math.max(0, Math.min(1, value));
const GOOGLE_GSI_SRC = "https://accounts.google.com/gsi/client";
let googleScriptPromise;

const ensureGoogleScript = () => {
  if (typeof window === "undefined") {
    return Promise.resolve();
  }
  if (window.google?.accounts?.id) {
    return Promise.resolve();
  }
  if (googleScriptPromise) {
    return googleScriptPromise;
  }
  const existing = document.querySelector(`script[src="${GOOGLE_GSI_SRC}"]`);
  if (existing) {
    googleScriptPromise = new Promise((resolve, reject) => {
      existing.addEventListener("load", () => resolve(), { once: true });
      existing.addEventListener("error", () => reject(new Error("Failed to load Google sign-in")), { once: true });
    });
    return googleScriptPromise;
  }
  googleScriptPromise = new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = GOOGLE_GSI_SRC;
    script.async = true;
    script.defer = true;
    script.onload = () => resolve();
    script.onerror = () => reject(new Error("Failed to load Google sign-in"));
    document.head.appendChild(script);
  });
  return googleScriptPromise;
};

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

const TooltipBubble = (props) => (
  <span class={`pointer-events-none absolute ${props.position || "bottom-full left-1/2 mb-2 -translate-x-1/2"} z-20 whitespace-nowrap border border-[var(--line)] bg-[var(--bg)] px-2 py-1 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--fg)] opacity-0 shadow-lg transition-opacity duration-75 group-hover:opacity-100 group-focus-within:opacity-100`}>
    {props.text}
  </span>
);

const PLAYBACK_SPEEDS = [1, 1.25, 1.5, 2];
const formatPlaybackSpeed = (value) => `${Number(value).toFixed(value % 1 === 0 ? 0 : 2).replace(/\.?0+$/, "")}x`;

const IconButton = (props) => (
  <span class="group relative inline-flex">
    <button
      type="button"
      onClick={props.disabled ? undefined : props.onClick}
      aria-label={props.label}
      disabled={props.disabled}
      class={`transition-colors ${
        props.disabled
          ? "cursor-not-allowed text-[var(--line)]"
          : props.active
            ? "text-[var(--fg)]"
            : "text-[var(--soft)] hover:text-[var(--fg)]"
      } ${props.class || ""}`}
    >
      {props.children}
    </button>
    <TooltipBubble text={props.label} />
  </span>
);

const HelpIcon = () => (
  <svg viewBox="0 0 24 24" class="h-4 w-4 fill-current">
    <path d="M12 3a9 9 0 1 0 9 9 9 9 0 0 0-9-9zm.1 14.4a1.1 1.1 0 1 1 0-2.2 1.1 1.1 0 0 1 0 2.2zm1.7-7.1-.8.6a2 2 0 0 0-.9 1.7v.4h-1.8v-.5a3.4 3.4 0 0 1 1.5-2.9l1-.7a1.6 1.6 0 0 0 .7-1.3 1.8 1.8 0 0 0-3.6.2H8.1a3.6 3.6 0 1 1 7.2-.2 3.3 3.3 0 0 1-1.5 2.7z" />
  </svg>
);

const PlusIcon = () => (
  <svg viewBox="0 0 24 24" class="h-4 w-4 fill-current">
    <path d="M11 5h2v14h-2zM5 11h14v2H5z" />
  </svg>
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

const LoadingSpinnerIcon = () => (
  <svg viewBox="0 0 24 24" class="h-4 w-4 animate-spin fill-none stroke-current stroke-2">
    <path d="M12 3a9 9 0 1 0 9 9" stroke-linecap="round" />
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

const ChevronDownIcon = () => (
  <svg viewBox="0 0 24 24" class="h-4 w-4 fill-none stroke-current stroke-2">
    <path d="m6 9 6 6 6-6" stroke-linecap="round" stroke-linejoin="round" />
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

const SpeedIcon = (props) => (
  <span class="inline-flex h-5 min-w-[2.1rem] items-center justify-center rounded-full border border-current px-1 font-mono text-[9px] uppercase leading-none">
    {formatPlaybackSpeed(props.speed)}
  </span>
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

const defaultUserPreferences = () => ({
  themePreference: "system",
  mainTab: "library",
  recentSongIds: [],
  playerVolume: 0.9,
  playerMuted: false,
  playbackSpeed: 1,
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
  const [playbackSpeed, setPlaybackSpeed] = createSignal(1);
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
  const [localMode, setLocalMode] = createSignal(false);
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
  const [adminUsers, setAdminUsers] = createSignal([]);
  const [airflowStatus, setAirflowStatus] = createSignal(null);
  const [adminMessage, setAdminMessage] = createSignal("");
  const [adminTab, setAdminTab] = createSignal("playlists");
  const [globalPlaylistNameEdit, setGlobalPlaylistNameEdit] = createSignal("");
  const [radioSaveMode, setRadioSaveMode] = createSignal("overwrite");
  const [radioSaveName, setRadioSaveName] = createSignal("");
  const [searchTab, setSearchTab] = createSignal("songs");
  const [globalPlaylistDetail, setGlobalPlaylistDetail] = createSignal(null);
  const [playlistDetailCache, setPlaylistDetailCache] = createSignal(new Map());
  const [playlistDetailLoading, setPlaylistDetailLoading] = createSignal(false);
  const [playlistDetailError, setPlaylistDetailError] = createSignal("");
  const [playlistSearchQuery, setPlaylistSearchQuery] = createSignal("");
  const [radioSearchQuery, setRadioSearchQuery] = createSignal("");
  const [googleReady, setGoogleReady] = createSignal(false);
  const [googleInitialized, setGoogleInitialized] = createSignal(false);
  const [preferencesReady, setPreferencesReady] = createSignal(false);
  const [preferenceStore, setPreferenceStore] = createSignal("pending");
  const [spotifyAuth, setSpotifyAuth] = createSignal(null);
  const [spotifyPlaylists, setSpotifyPlaylists] = createSignal([]);
  const [selectedSpotifyPlaylistId, setSelectedSpotifyPlaylistId] = createSignal("");
  const [showShortcutHelp, setShowShortcutHelp] = createSignal(false);
  const [showCreatePlaylistModal, setShowCreatePlaylistModal] = createSignal(false);
  const [showAuthPrompt, setShowAuthPrompt] = createSignal(false);
  const [showProfileMenu, setShowProfileMenu] = createSignal(false);
  const [loadingFrame, setLoadingFrame] = createSignal(0);
  const [pendingRadioOffset, setPendingRadioOffset] = createSignal(null);
  const [pendingPlaylistSongId, setPendingPlaylistSongId] = createSignal("");
  const [appOffline, setAppOffline] = createSignal(false);
  const [offlineMessage, setOfflineMessage] = createSignal("");
  const [cacheStatus, setCacheStatus] = createSignal(null);
  const [cacheTrimming, setCacheTrimming] = createSignal(false);
  const [cacheMessage, setCacheMessage] = createSignal("");
  const [showSettings, setShowSettings] = createSignal(false);
  const [dbSyncState, setDbSyncState] = createSignal(null);
  const [configReady, setConfigReady] = createSignal(false);

  let worker;
  const audioRefs = [];
  let googleButtonRef;
  let profileMenuRef;
  let profileMenuButtonRef;
  let listRef;
  let searchInputRef;
  let createPlaylistInputRef;
  let searchTimeout;
  let removeKeydownListener = null;
  let removePointerdownListener = null;
  let removeOnlineListener = null;
  let removeOfflineListener = null;
  let prefetchTimer;
  let keyboardNavTimer;
  let scrollAnimationFrame;
  let adminRefreshTimer;
  let loadingTimer;
  let radioSyncTimer;
  let healthPollTimer;
  let cachePollTimer;
  let dbSyncPollTimer;
  let healthFailCount = 0;
  let crossfadeFrame;
  let themeMediaQuery;
  let syncSystemTheme;
  let crossfadeToken = 0;
  let playlistDetailRequestToken = 0;
  let activeDeckIndex = 0;
  let fadingAudio = null;
  const prefetchedIds = new Set();
  const rowRefs = new Map();

  const cachePercent = createMemo(() => {
    const status = cacheStatus();
    if (!status || !status.limitBytes) return 0;
    return Math.min(100, Math.round((status.usageBytes / status.limitBytes) * 100));
  });
  const cacheNearFull = createMemo(() => cachePercent() >= 80);
  const cacheFull = createMemo(() => cachePercent() >= 95);

  const easeOutQuint = (value) => 1 - (1 - value) ** 5;
  const authEnabled = createMemo(() => configReady() && !localMode());
  const libraryProfileEnabled = createMemo(() => configReady() && (localMode() || Boolean(user())));
  const radioEnabled = createMemo(() => configReady() && !localMode());
  const spotifyEnabled = createMemo(() => authEnabled() && Boolean(spotifyClientId()));
  const visiblePlaylistDetail = createMemo(() => {
    if (query().trim() || movieFilter() || artistFilter()) {
      return null;
    }
    return globalPlaylistDetail();
  });
  const canManageVisiblePlaylist = createMemo(() => {
    const playlist = visiblePlaylistDetail();
    const account = user();
    if (!playlist || !account) {
      return false;
    }
    if (playlist.isGlobal) {
      return Boolean(account.is_admin);
    }
    return true;
  });
  const localDbSyncLabel = createMemo(() => {
    const sync = dbSyncState();
    if (!localMode() || !sync?.enabled) {
      return "";
    }
    if (sync.status === "checking") {
      return "Checking library";
    }
    if (sync.status === "downloading") {
      if (sync.totalBytes > 0 && sync.downloadedBytes > 0) {
        const percent = Math.min(100, Math.round((sync.downloadedBytes / sync.totalBytes) * 100));
        return `Updating library ${percent}%`;
      }
      return "Updating library";
    }
    if (sync.status === "error") {
      return "Library sync error";
    }
    return "";
  });
  const localDbSyncTone = createMemo(() => {
    const status = dbSyncState()?.status;
    if (status === "error") {
      return "border-[#d36b6b] text-[#d36b6b]";
    }
    if (status === "checking" || status === "downloading") {
      return "border-[var(--brand)] text-[var(--brand)]";
    }
    return "border-[var(--line)] text-[var(--soft)]";
  });

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
      .slice(0, 8);
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

  const updatePlaylistSummary = (playlistId, patch) => {
    if (!playlistId) {
      return;
    }
    setPlaylists((current) => current.map((playlist) => {
      if (playlist.id !== playlistId) {
        return playlist;
      }
      const nextPatch = typeof patch === "function" ? patch(playlist) : patch;
      return { ...playlist, ...(nextPatch || {}) };
    }));
  };

  const appendTrackToPlaylistCache = (playlistId, track, nextTrackCount = null) => {
    if (!playlistId || !track) {
      return;
    }
    const applyTrack = (playlist) => {
      if (!playlist || playlist.id !== playlistId) {
        return playlist;
      }
      const tracks = Array.isArray(playlist.tracks) ? playlist.tracks : [];
      if (tracks.some((item) => item.id === track.id)) {
        return nextTrackCount == null ? playlist : { ...playlist, trackCount: nextTrackCount };
      }
      return {
        ...playlist,
        trackCount: nextTrackCount == null ? tracks.length + 1 : nextTrackCount,
        tracks: [...tracks, track],
      };
    };

    setPlaylistDetailCache((current) => {
      if (!current.has(playlistId)) {
        return current;
      }
      const next = new Map(current);
      next.set(playlistId, applyTrack(next.get(playlistId)));
      return next;
    });
    setGlobalPlaylistDetail((current) => applyTrack(current));
  };

  const markAppOffline = (message) => {
    setAppOffline(true);
    setOfflineMessage(message || "App is offline. Please check your internet connection or restart Docker.");
  };

  const markAppOnline = () => {
    setAppOffline(false);
    setOfflineMessage("");
  };

  const verifyAppOnline = async () => {
    if (typeof navigator !== "undefined" && navigator.onLine === false) {
      healthFailCount = 3;
      markAppOffline("No internet connection detected.");
      return false;
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000);
    try {
      const response = await fetch("/api/health", {
        cache: "no-store",
        signal: controller.signal,
      });
      if (!response.ok) {
        healthFailCount++;
        if (healthFailCount >= 3) {
          markAppOffline("Backend is not responding. Docker may need a restart.");
        }
        return false;
      }
      if (healthFailCount > 0) {
        healthFailCount = 0;
      }
      if (appOffline()) {
        markAppOnline();
      }
      return true;
    } catch {
      healthFailCount++;
      if (healthFailCount >= 3) {
        markAppOffline("Backend is not responding. Docker may need a restart.");
      }
      return false;
    } finally {
      clearTimeout(timeoutId);
    }
  };

  const refreshCacheStatus = async () => {
    if (!localMode()) return;
    try {
      const response = await fetch("/api/cache/status", { cache: "no-store" });
      if (response.ok) {
        const payload = await response.json();
        setCacheStatus(payload);
      }
    } catch {}
  };

  const trimCache = async (force = false) => {
    setCacheTrimming(true);
    setCacheMessage("");
    try {
      const response = await fetch("/api/cache/trim", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ force }),
      });
      const payload = await response.json();
      if (!response.ok) {
        setCacheMessage(payload.message || "Unable to clear cache");
        return;
      }
      const removedMb = (payload.removedBytes / (1024 * 1024)).toFixed(1);
      setCacheMessage(
        payload.removedFiles > 0
          ? `Cleared ${payload.removedFiles} files (${removedMb} MB)`
          : "Cache is already within limits"
      );
      await refreshCacheStatus();
    } catch (err) {
      setCacheMessage(err?.message || "Unable to clear cache");
    } finally {
      setCacheTrimming(false);
    }
  };

  const refreshDbSyncStatus = async () => {
    if (!localMode()) {
      setDbSyncState(null);
      return;
    }
    try {
      const response = await fetch("/api/db-sync/status", { cache: "no-store" });
      if (!response.ok) {
        throw new Error("Unable to read library sync status");
      }
      const payload = await response.json();
      setDbSyncState(payload.sync || null);
    } catch (syncError) {
      setDbSyncState((current) => current || {
        enabled: true,
        status: "error",
        message: syncError?.message || "Unable to read library sync status",
        githubIssuesUrl: "https://github.com/rgcsekaraa/isaibox/issues",
      });
    }
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
  const normalizedPlaylistSearch = createMemo(() => playlistSearchQuery().trim().toLowerCase());
  const normalizedRadioSearch = createMemo(() => radioSearchQuery().trim().toLowerCase());
  const filteredUserPlaylists = createMemo(() => {
    const needle = normalizedPlaylistSearch();
    if (!needle) {
      return playlists();
    }
    return playlists().filter((playlist) => (playlist.name || "").toLowerCase().includes(needle));
  });
  const filteredGlobalPlaylists = createMemo(() => {
    const needle = normalizedPlaylistSearch();
    if (!needle) {
      return globalPlaylists();
    }
    return globalPlaylists().filter((playlist) =>
      [playlist.name, playlist.source].some((value) => (value || "").toLowerCase().includes(needle))
    );
  });
  const filteredRadioStations = createMemo(() => {
    const needle = normalizedRadioSearch();
    if (!needle) {
      return radioStations();
    }
    return radioStations().filter((station) =>
      [station.name, station.blurb, station.yearStart, station.yearEnd]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(needle))
    );
  });
  const showPlaylistDetail = createMemo(() => Boolean(visiblePlaylistDetail()));
  const currentRadioStation = createMemo(() => radioStations().find((station) => station.id === selectedRadioStationId()) || null);
  const radioPlaybackLocked = createMemo(() => mainTab() === "radio");
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
    if (mainTab() === "admin") {
      return [];
    }
    if (mainTab() === "playlists") {
      return globalPlaylistDetail()?.tracks || [];
    }
    if (mainTab() === "library" && showPlaylistDetail()) {
      return globalPlaylistDetail()?.tracks || [];
    }
    return visibleResults();
  });
  const selectedSong = createMemo(() => {
    const visible = activeSongList();
    return (
      visible.find((song) => song.id === selectedId()) ||
      (globalPlaylistDetail()?.tracks || []).find((song) => song.id === selectedId()) ||
      songs().find((song) => song.id === selectedId()) ||
      null
    );
  });
  const currentSong = createMemo(() => {
    const currentId = currentTrackId();
    if (!currentId) {
      return null;
    }
    return (
      activeSongList().find((song) => song.id === currentId) ||
      (globalPlaylistDetail()?.tracks || []).find((song) => song.id === currentId) ||
      songs().find((song) => song.id === currentId) ||
      null
    );
  });
  const selectedActiveSong = createMemo(() => activeSongList().find((song) => song.id === selectedId()) || null);
  const selectedIndex = createMemo(() => activeSongList().findIndex((song) => song.id === selectedId()));
  const favoriteIdSet = createMemo(() => new Set(favoriteIds()));
  const loadingDots = createMemo(() => ".".repeat((loadingFrame() % 3) + 1));
  const libraryPlaylistCards = createMemo(() => [
    ...playlists().map((playlist) => ({ ...playlist, section: "Yours" })),
    ...globalPlaylists().map((playlist) => ({ ...playlist, section: "Global" })),
  ]);
  const playlistSummaryById = createMemo(() => {
    const map = new Map();
    for (const playlist of playlists()) {
      if (playlist?.id) {
        map.set(playlist.id, playlist);
      }
    }
    for (const playlist of globalPlaylists()) {
      if (playlist?.id) {
        map.set(playlist.id, playlist);
      }
    }
    return map;
  });
  const focusSearch = () => {
    setMainBrowseTab("library");
    setSearchTab("songs");
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        searchInputRef?.focus();
        searchInputRef?.select?.();
      });
    });
  };
  const isEditableTarget = (target) => {
    const tagName = target?.tagName?.toLowerCase?.() || "";
    return tagName === "input" || tagName === "textarea" || tagName === "select" || target?.isContentEditable;
  };
  const activateMainTabShortcut = (tab) => {
    if (tab === "admin" && !user()?.is_admin) {
      return;
    }
    if (tab === "playlists" && !localMode()) {
      return;
    }
    if (tab === "radio" && !radioEnabled()) {
      return;
    }
    if (tab === "favorites" && !user()) {
      return;
    }
    setMainBrowseTab(tab);
  };
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

  const applyUserPreferences = (payload = {}) => {
    const defaults = defaultUserPreferences();
    const next = {
      ...defaults,
      ...(payload || {}),
    };
    const nextThemePreference = ["system", "light", "dark"].includes(next.themePreference) ? next.themePreference : defaults.themePreference;
    const allowedMainTabs = radioEnabled()
      ? ["library", "recents", "favorites", "radio", "admin"]
      : ["library", "recents", "favorites"];
    const requestedMainTab = allowedMainTabs.includes(next.mainTab) ? next.mainTab : defaults.mainTab;
    const nextMainTab = !radioEnabled() && requestedMainTab === "radio" ? defaults.mainTab : requestedMainTab;
    const nextRepeatMode = ["off", "one", "album", "random"].includes(next.repeatMode) ? next.repeatMode : defaults.repeatMode;
    const nextRecentSongIds = Array.isArray(next.recentSongIds) ? next.recentSongIds.filter((id) => typeof id === "string" && id).slice(0, 20) : [];
    const nextPlayerVolume = Number.isFinite(Number(next.playerVolume)) ? clampUnit(Number(next.playerVolume)) : defaults.playerVolume;
    const requestedPlaybackSpeed = Number(next.playbackSpeed);
    const nextPlaybackSpeed = PLAYBACK_SPEEDS.includes(requestedPlaybackSpeed) ? requestedPlaybackSpeed : defaults.playbackSpeed;

    setThemePreference(nextThemePreference);
    setMainTab(nextMainTab);
    setRecentIds(nextRecentSongIds);
    setVolume(nextPlayerVolume);
    setMuted(Boolean(next.playerMuted));
    setPlaybackSpeed(nextPlaybackSpeed);
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
    playbackSpeed: playbackSpeed(),
    repeatMode: repeatMode(),
    autoplayNext: autoplayNext(),
  });

  const mergePreferences = (base = {}, incoming = {}) => {
    const defaults = defaultUserPreferences();
    return {
      ...defaults,
      ...base,
      ...incoming,
      recentSongIds: [...new Set([...(incoming.recentSongIds || []), ...(base.recentSongIds || [])])].slice(0, 20),
    };
  };

  const toggleThemePreference = () => {
    setThemePreference(theme() === "dark" ? "light" : "dark");
  };

  const setThemePreferenceChoice = (nextTheme) => {
    if (!["light", "system", "dark"].includes(nextTheme)) {
      return;
    }
    setThemePreference(nextTheme);
    setShowProfileMenu(false);
  };

  const resolveRadioStationPlayback = (station) => {
    const songIds = station?.songIds || [];
    const trackCount = songIds.length;
    if (!trackCount) {
      return {
        queue: [],
        currentIndex: 0,
        currentSongId: "",
        currentOffsetSeconds: 0,
        sharedSongSeconds: Number(station?.sharedSongSeconds || 0),
      };
    }

    const baseIndex = Math.min(trackCount - 1, Math.max(0, Number(station?.currentIndex || 0)));
    const sharedSongSeconds = Math.max(1, Number(station?.sharedSongSeconds || 0) || 0);
    const slotStartedAtMs = Date.parse(station?.slotStartedAt || "");
    if (!Number.isFinite(slotStartedAtMs) || sharedSongSeconds <= 0) {
      return {
        queue: songIds,
        currentIndex: baseIndex,
        currentSongId: songIds[baseIndex] || "",
        currentOffsetSeconds: Math.max(0, Number(station?.currentOffsetSeconds || 0)),
        sharedSongSeconds,
      };
    }

    const elapsedSeconds = Math.max(0, Math.floor((Date.now() - slotStartedAtMs) / 1000));
    const slotAdvance = Math.floor(elapsedSeconds / sharedSongSeconds);
    const currentIndex = (baseIndex + slotAdvance) % trackCount;
    const currentOffsetSeconds = elapsedSeconds % sharedSongSeconds;

    return {
      queue: songIds,
      currentIndex,
      currentSongId: songIds[currentIndex] || "",
      currentOffsetSeconds,
      sharedSongSeconds,
    };
  };

  const advanceRadioStationLocally = (stationId) => {
    setRadioStations((current) => current.map((station) => {
      if (station.id !== stationId || !station.songIds?.length) {
        return station;
      }
      const playback = resolveRadioStationPlayback(station);
      const nextIndex = (playback.currentIndex + 1) % station.songIds.length;
      return {
        ...station,
        currentIndex: nextIndex,
        currentSongId: station.songIds[nextIndex] || "",
        currentOffsetSeconds: 0,
        slotStartedAt: new Date().toISOString(),
      };
    }));
  };

  const applyRadioStation = (stationId, autoplay = false) => {
    const station = radioStations().find((item) => item.id === stationId) || radioStations()[0];
    if (!station) {
      setRadioQueue([]);
      setRadioMessage("No radio stations available");
      return;
    }
    const playback = resolveRadioStationPlayback(station);
    const queue = (playback.queue || []).map((id) => songIndex().get(id)).filter(Boolean);
    const currentIndex = Math.min(queue.length - 1, Math.max(0, playback.currentIndex || 0));
    const currentSong = queue[currentIndex] || queue[0] || null;
    setSelectedRadioStationId(station.id);
    setRadioQueue(queue);
    setMainTab("radio");
    if (currentSong) {
      setSelectedId(currentSong.id);
      setPendingRadioOffset({
        songId: currentSong.id,
        offsetSeconds: Number(playback.currentOffsetSeconds || 0),
        slotSeconds: Number(playback.sharedSongSeconds || 0),
      });
      if (autoplay) {
        loadSong(currentSong, true);
      }
    }
  };

  const fetchRadioStations = async (forceRefresh = false, autoplayAfterLoad = false) => {
    if (!radioEnabled()) {
      return;
    }
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
      if (mainTab() === "radio" || autoplayAfterLoad) {
        applyRadioStation(nextStationId, mainTab() === "radio" || autoplayAfterLoad);
      }
    } catch (fetchError) {
      setRadioMessage(fetchError?.message || "Unable to build radio stations");
      setRadioQueue([]);
    } finally {
      setRadioLoading(false);
    }
  };

  const setMainBrowseTab = (tab) => {
    if (tab === "radio" && !radioEnabled()) {
      tab = "library";
    }
    setMainTab(tab);
    if (tab === "admin" || tab === "playlists") {
      return;
    }
    if (tab === "radio" && !radioQueue().length && songs().length) {
      if (radioStations().length) {
        applyRadioStation(selectedRadioStationId() || radioStations()[0]?.id || "", true);
      } else if (!radioLoading()) {
        void fetchRadioStations();
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
    if (!songId || mainTab() === "radio") {
      return;
    }
    setRecentIds((current) => [songId, ...current.filter((id) => id !== songId)].slice(0, 20));
  };

  const clearRecents = () => {
    setRecentIds([]);
  };

  const startRadio = () => {
    if (!radioEnabled()) {
      return;
    }
    if (!radioStations().length) {
      void fetchRadioStations(true, true);
      return;
    }
    applyRadioStation(selectedRadioStationId() || radioStations()[0]?.id || "", true);
  };

  const saveRadioStationWithMode = async () => {
    if (!user()?.is_admin || !currentRadioStation()) {
      return;
    }
    const body = {
      mode: radioSaveMode(),
      targetPlaylistId: radioSaveMode() === "overwrite" ? selectedGlobalPlaylistTarget() : "",
      name: radioSaveName().trim() || currentRadioStation().name,
    };
    setAdminMessage("Saving radio station...");
    const response = await fetch(`/api/admin/radio-stations/${currentRadioStation().id}/playlist`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const payload = await response.json();
    if (!response.ok) {
      setAdminMessage(payload.message || "Unable to save radio station");
      return;
    }
    await refreshAccountState();
    await openGlobalPlaylist(payload.playlist.id);
    setAdminMessage(`${payload.playlist.updatedExisting ? "Updated" : "Saved"} global playlist: ${payload.playlist.name}`);
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

  const syncDeckPlaybackSpeed = () => {
    audioRefs.forEach((audio) => {
      if (!audio) {
        return;
      }
      audio.playbackRate = playbackSpeed();
      audio.defaultPlaybackRate = playbackSpeed();
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
      const [sessionResponse, playlistsResponse] = await Promise.all([
        fetch("/api/auth/session"),
        fetch("/api/playlists"),
      ]);
      const sessionPayload = await sessionResponse.json();
      const sessionUser = sessionPayload.user || null;
      setUser(sessionUser);

      if (!sessionUser) {
        if (playlistsResponse.ok) {
          const playlistsPayload = await playlistsResponse.json();
          const nextPlaylists = playlistsPayload.playlists || [];
          const nextGlobalPlaylists = playlistsPayload.globalPlaylists || [];
          setPlaylists(nextPlaylists);
          setGlobalPlaylists(nextGlobalPlaylists);
          if (globalPlaylistDetail()) {
            const currentGlobal = [...nextPlaylists, ...nextGlobalPlaylists].find((playlist) => playlist.id === globalPlaylistDetail()?.id);
            if (!currentGlobal) {
              setGlobalPlaylistDetail(null);
            }
          }
          if (!selectedPlaylistTarget() && nextPlaylists[0]?.id) {
            setSelectedPlaylistTarget(nextPlaylists[0].id);
          }
          const defaultPlaylistId = globalPlaylistDetail()?.id || selectedGlobalPlaylistTarget() || nextPlaylists[0]?.id || nextGlobalPlaylists[0]?.id || "";
          if (defaultPlaylistId) {
            setSelectedGlobalPlaylistTarget(defaultPlaylistId);
            if (!globalPlaylistDetail()) {
              void openGlobalPlaylist(defaultPlaylistId);
            }
          }
        } else {
          setPlaylists([]);
          setGlobalPlaylists([]);
        }
        setFavoriteIds([]);
        setPlaylists([]);
        setPlaylistDetailCache(new Map());
        setPlaylistDetailLoading(false);
        setPlaylistDetailError("");
        setSelectedPlaylistTarget("");
        setAdminUsers([]);
        setAirflowStatus(null);
        setPreferenceStore("guest");
        resetUserScopedPreferences();
        return;
      }

      const [preferencesResponse, favoritesResponse] = await Promise.all([
        fetch("/api/me/preferences"),
        fetch("/api/favorites"),
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
        const nextPlaylists = playlistsPayload.playlists || [];
        const nextGlobalPlaylists = playlistsPayload.globalPlaylists || [];
        setPlaylists(nextPlaylists);
        setGlobalPlaylists(nextGlobalPlaylists);
        setPlaylistDetailCache((current) => {
          const next = new Map(current);
          const visibleIds = new Set([
            ...nextPlaylists.map((playlist) => playlist.id).filter(Boolean),
            ...nextGlobalPlaylists.map((playlist) => playlist.id).filter(Boolean),
          ]);
          Array.from(next.keys()).forEach((id) => {
            if (!visibleIds.has(id)) {
              next.delete(id);
            }
          });
          return next;
        });
        if (!selectedPlaylistTarget() && nextPlaylists[0]?.id) {
          setSelectedPlaylistTarget(nextPlaylists[0].id);
        }
        const defaultPlaylistId = globalPlaylistDetail()?.id || selectedGlobalPlaylistTarget() || nextPlaylists[0]?.id || nextGlobalPlaylists[0]?.id || "";
        if (globalPlaylistDetail()) {
          const detailId = globalPlaylistDetail()?.id;
          const detailStillVisible = [...nextPlaylists, ...nextGlobalPlaylists]
            .some((playlist) => playlist.id === detailId);
          if (!detailStillVisible) {
            setGlobalPlaylistDetail(null);
            setPlaylistDetailError("");
            setPlaylistDetailLoading(false);
          }
        }
        if (defaultPlaylistId) {
          setSelectedGlobalPlaylistTarget(defaultPlaylistId);
          if (!globalPlaylistDetail()) {
            void openGlobalPlaylist(defaultPlaylistId);
          }
        }
        if (!globalPlaylistDetail()) {
          const defaultPlaylistSummary = [...nextPlaylists, ...nextGlobalPlaylists].find((playlist) => playlist.id === defaultPlaylistId);
          if (defaultPlaylistSummary) {
            setGlobalPlaylistNameEdit(defaultPlaylistSummary.name || "");
          }
        }
      } else {
        setPlaylists([]);
        setGlobalPlaylists([]);
        setPlaylistDetailCache(new Map());
        setPlaylistDetailLoading(false);
        setPlaylistDetailError("");
        setSelectedPlaylistTarget("");
        setSelectedGlobalPlaylistTarget("");
      }
    } catch {
      setUser(null);
      setFavoriteIds([]);
      setPlaylists([]);
      setGlobalPlaylists([]);
      setPlaylistDetailCache(new Map());
      setPlaylistDetailLoading(false);
      setPlaylistDetailError("");
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

  const debugSpotifySession = async () => {
    const auth = spotifyAuth();
    if (!auth?.accessToken) {
      setAccountMessage("Spotify debug: no access token in browser session");
      return;
    }
    const expiresInSeconds = Math.max(0, Math.floor((auth.expiresAt - Date.now()) / 1000));
    let accessToken = "";
    try {
      accessToken = await ensureSpotifyAccessToken();
    } catch (error) {
      setAccountMessage(`Spotify debug: token refresh failed (${error?.message || "unknown error"})`);
      return;
    }

    try {
      const response = await fetch("/api/spotify/playlists", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ accessToken }),
      });
      const payload = await response.json();
      if (!response.ok) {
        setAccountMessage(
          `Spotify debug: token ok, playlist API failed (${payload.message || "unknown error"}) | expires in ${expiresInSeconds}s | scopes: ${auth.scope || "none"}`
        );
        return;
      }
      setAccountMessage(
        `Spotify debug: token ok, loaded ${payload.playlists?.length || 0} playlists | expires in ${expiresInSeconds}s | scopes: ${auth.scope || "none"}`
      );
    } catch (error) {
      setAccountMessage(`Spotify debug: request failed (${error?.message || "unknown error"})`);
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
      setAccountMessage("Create an account to save favorites");
      setShowAuthPrompt(true);
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
    if (!user()) {
      setAccountMessage("Create an account to make playlists");
      setShowAuthPrompt(true);
      return;
    }
    if (!name) {
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
    const pendingSongId = pendingPlaylistSongId();
    setPlaylistNameInput("");
    setPlaylists((current) => [payload.playlist, ...current]);
    setSelectedPlaylistTarget(payload.playlist.id);
    if (pendingSongId) {
      setPendingPlaylistSongId("");
      setShowCreatePlaylistModal(false);
      const song = songIndex().get(pendingSongId);
      if (song) {
        await addSongToPlaylistById(payload.playlist.id, song);
        return;
      }
    }
    setShowCreatePlaylistModal(false);
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

  const addSongToPlaylistById = async (playlistId, song) => {
    if (!user() || !playlistId || !song) {
      return false;
    }
    const response = await fetch(`/api/playlists/${playlistId}/songs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ songId: song.id }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      setAccountMessage(payload.message || "Unable to add song to playlist");
      return false;
    }
    const nextTrackCount = payload.playlist?.trackCount;
    updatePlaylistSummary(playlistId, (playlist) => ({
      trackCount: Number.isFinite(Number(nextTrackCount)) ? Number(nextTrackCount) : playlist.trackCount,
    }));
    appendTrackToPlaylistCache(playlistId, payload.track, Number.isFinite(Number(nextTrackCount)) ? Number(nextTrackCount) : null);
    setSelectedPlaylistTarget(playlistId);
    setAccountMessage(payload.alreadyExists ? "Already in playlist" : "Saved to playlist");
    return true;
  };

  const addCurrentToPlaylist = async () => {
    if (!user() || !selectedPlaylistTarget() || !currentSong()) {
      if (!user()) {
        setAccountMessage("Create an account to save songs into playlists");
        setShowAuthPrompt(true);
      }
      return;
    }
    await addSongToPlaylistById(selectedPlaylistTarget(), currentSong());
  };

  const saveCurrentToPlaylist = async () => {
    if (!user()) {
      setAccountMessage("Create an account to save songs into playlists");
      setShowAuthPrompt(true);
      return;
    }
    if (!currentSong()) {
      return;
    }
    if (!playlists().length) {
      setPendingPlaylistSongId(currentSong().id);
      setPlaylistNameInput(currentSong().movie ? `${currentSong().movie} picks` : "");
      setShowCreatePlaylistModal(true);
      requestAnimationFrame(() => {
        requestAnimationFrame(() => createPlaylistInputRef?.focus());
      });
      return;
    }
    if (playlists().length === 1) {
      await addSongToPlaylistById(playlists()[0].id, currentSong());
      return;
    }
    const playlistId = selectedPlaylistTarget() || playlists()[0]?.id || "";
    if (!playlistId) {
      setPendingPlaylistSongId(currentSong().id);
      setShowCreatePlaylistModal(true);
      return;
    }
    setPendingPlaylistSongId(currentSong().id);
    setSelectedPlaylistTarget(playlistId);
    setShowCreatePlaylistModal(true);
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
    if (!spotifyConnected()) {
      setAccountMessage("Connect Spotify first to import playlist links");
      return;
    }
    setAccountMessage("Importing Spotify playlist...");
    const accessToken = await ensureSpotifyAccessToken().catch(() => "");
    if (!accessToken) {
      setAccountMessage("Spotify session unavailable. Reconnect Spotify and try again");
      return;
    }
    const response = await fetch("/api/playlists/import/spotify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: spotifyImportUrl().trim(), accessToken }),
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
    if (!user()?.is_admin) {
      return;
    }
    setAccountMessage("Generating global AI playlists...");
    const response = await fetch("/api/admin/playlists/ai/generate", { method: "POST" });
    const payload = await response.json();
    if (!response.ok) {
      setAccountMessage(payload.message || "Unable to generate AI playlists");
      return;
    }
    await refreshAccountState();
    setAccountMessage(`Generated ${payload.playlists?.length || 0} global AI playlists${payload.source === "gemini" ? "" : " with local fallback"}`);
  };

  const openGlobalPlaylist = async (playlistId) => {
    if (!playlistId) {
      return;
    }
    const requestToken = ++playlistDetailRequestToken;
    setQuery("");
    setMovieFilter("");
    setArtistFilter("");
    setSearchTab("songs");
    setSelectedGlobalPlaylistTarget(playlistId);
    setPlaylistDetailError("");

    const playlistSummary = playlistSummaryById().get(playlistId);
    const cachedPlaylist = playlistDetailCache().get(playlistId);
    const cacheLooksFresh = Boolean(cachedPlaylist) && (
      !playlistSummary ||
      (
        (cachedPlaylist.updatedAt || "") === (playlistSummary.updatedAt || "") &&
        (cachedPlaylist.tracks || []).length === Number(playlistSummary.trackCount || 0)
      )
    );
    if (cacheLooksFresh) {
      setGlobalPlaylistDetail(cachedPlaylist);
      setGlobalPlaylistNameEdit(cachedPlaylist.name || "");
      setPlaylistDetailLoading(false);
      prefetchSongIds((cachedPlaylist.tracks || []).slice(0, 8).map((track) => track.id));
      return;
    }

    if (cachedPlaylist) {
      setGlobalPlaylistDetail(cachedPlaylist);
      setGlobalPlaylistNameEdit(cachedPlaylist.name || "");
    }

    setPlaylistDetailLoading(true);
    try {
      const response = await fetch(`/api/playlists/${playlistId}`);
      const payload = await response.json().catch(() => ({}));
      if (requestToken !== playlistDetailRequestToken) {
        return;
      }
      if (!response.ok) {
        setGlobalPlaylistDetail(null);
        setPlaylistDetailError(payload.message || "Unable to load playlist");
        setAccountMessage(payload.message || "Unable to load playlist");
        return;
      }
      const nextPlaylist = payload.playlist && typeof payload.playlist === "object"
        ? {
            ...payload.playlist,
            tracks: Array.isArray(payload.playlist.tracks) ? payload.playlist.tracks : [],
          }
        : null;
      if (!nextPlaylist) {
        setGlobalPlaylistDetail(null);
        setPlaylistDetailError("Playlist payload was invalid");
        return;
      }
      setPlaylistDetailCache((current) => {
        const next = new Map(current);
        next.set(nextPlaylist.id, nextPlaylist);
        return next;
      });
      setGlobalPlaylistDetail(nextPlaylist);
      setGlobalPlaylistNameEdit(nextPlaylist.name || "");
      setSelectedGlobalPlaylistTarget(nextPlaylist.id || playlistId);
      prefetchSongIds((nextPlaylist.tracks || []).slice(0, 8).map((track) => track.id));
    } catch (error) {
      if (requestToken !== playlistDetailRequestToken) {
        return;
      }
      setGlobalPlaylistDetail(null);
      setPlaylistDetailError(error?.message || "Unable to load playlist");
      setAccountMessage(error?.message || "Unable to load playlist");
    } finally {
      if (requestToken === playlistDetailRequestToken) {
        setPlaylistDetailLoading(false);
      }
    }
  };

  const closeGlobalPlaylist = () => {
    setGlobalPlaylistDetail(null);
    setPlaylistDetailError("");
    setPlaylistDetailLoading(false);
    setQuery("");
    setMovieFilter("");
    setArtistFilter("");
    setSearchTab("songs");
  };

  const removeSongFromPlaylist = async (playlistId, songId) => {
    if (!canManageVisiblePlaylist()) {
      return;
    }
    const response = await fetch(`/api/playlists/${playlistId}/songs/${songId}`, { method: "DELETE" });
    if (!response.ok) {
      setAccountMessage("Unable to remove song from playlist");
      return;
    }
    await refreshAccountState();
    await openGlobalPlaylist(playlistId);
  };

  const clearVisiblePlaylist = async () => {
    const playlist = globalPlaylistDetail();
    if (!playlist || !canManageVisiblePlaylist()) {
      return;
    }
    const response = await fetch(`/api/playlists/${playlist.id}/songs`, { method: "DELETE" });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      setAccountMessage(payload.message || "Unable to clear playlist");
      return;
    }
    await refreshAccountState();
    await openGlobalPlaylist(playlist.id);
    setAccountMessage("Playlist cleared");
  };

  const deleteVisiblePlaylist = async () => {
    const playlist = globalPlaylistDetail();
    if (!playlist || !canManageVisiblePlaylist()) {
      return;
    }
    const response = await fetch(`/api/playlists/${playlist.id}`, { method: "DELETE" });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      setAccountMessage(payload.message || "Unable to delete playlist");
      return;
    }
    await refreshAccountState();
    closeGlobalPlaylist();
    setSelectedGlobalPlaylistTarget("");
    setAccountMessage("Playlist deleted");
  };

  const renamePlaylistLocal = async () => {
    if (!user() || !globalPlaylistDetail()) {
      return;
    }
    const response = await fetch(`/api/playlists/${globalPlaylistDetail().id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: globalPlaylistNameEdit().trim() }),
    });
    const payload = await response.json();
    if (!response.ok) {
      setAccountMessage(payload.message || "Unable to rename playlist");
      return;
    }
    await refreshAccountState();
    await openGlobalPlaylist(globalPlaylistDetail().id);
    setAccountMessage("Playlist renamed");
  };

  const renameGlobalPlaylist = async () => {
    if (!user()?.is_admin || !globalPlaylistDetail()) {
      return;
    }
    const response = await fetch(`/api/playlists/${globalPlaylistDetail().id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: globalPlaylistNameEdit().trim() }),
    });
    const payload = await response.json();
    if (!response.ok) {
      setAdminMessage(payload.message || "Unable to rename playlist");
      return;
    }
    await refreshAccountState();
    await openGlobalPlaylist(globalPlaylistDetail().id);
    setAdminMessage("Playlist renamed");
  };

  const moveGlobalPlaylistSong = async (songId, direction) => {
    if (!user()?.is_admin || !globalPlaylistDetail()) {
      return;
    }
    const tracks = [...(globalPlaylistDetail().tracks || [])];
    const index = tracks.findIndex((track) => track.id === songId);
    const nextIndex = index + direction;
    if (index < 0 || nextIndex < 0 || nextIndex >= tracks.length) {
      return;
    }
    [tracks[index], tracks[nextIndex]] = [tracks[nextIndex], tracks[index]];
    const response = await fetch(`/api/playlists/${globalPlaylistDetail().id}/songs/reorder`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ songIds: tracks.map((track) => track.id) }),
    });
    const payload = await response.json();
    if (!response.ok) {
      setAdminMessage(payload.message || "Unable to reorder playlist");
      return;
    }
    setGlobalPlaylistDetail((current) => current ? { ...current, tracks } : current);
    await refreshAccountState();
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
    return list.slice(index, index + 8).map((item) => item.id);
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

  const cyclePlaybackSpeed = () => {
    const currentIndex = PLAYBACK_SPEEDS.indexOf(playbackSpeed());
    const nextIndex = currentIndex >= 0 ? (currentIndex + 1) % PLAYBACK_SPEEDS.length : 0;
    setPlaybackSpeed(PLAYBACK_SPEEDS[nextIndex]);
  };

  const playbackModeLabel = createMemo(() => {
    if (repeatMode() === "one") return "Single song loop";
    if (repeatMode() === "album") return "Album loop";
    if (repeatMode() === "random") return "Random";
    return "Normal";
  });

  const playbackSpeedLabel = createMemo(() => `Playback speed: ${formatPlaybackSpeed(playbackSpeed())}`);

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

  const loadSong = (song, autoplay = false, options = {}) => {
    const { allowCrossfade = false } = options;
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
      const canCrossfade = Boolean(
        allowCrossfade &&
        autoplay &&
        activeAudio.src &&
        !activeAudio.paused &&
        currentTrackId() &&
        currentTrackId() !== song.id
      );
      stopCrossfade();
      setCurrentTime(0);
      setDuration(0);
      setStreamStarted(false);
      if (!canCrossfade) {
        activeAudio.pause();
        activeAudio.currentTime = 0;
        activeAudio.removeAttribute("src");
        activeAudio.dataset.songId = "";
        activeAudio.load();
      }
      inactiveAudio.pause();
      inactiveAudio.dataset.songId = song.id;
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
            const nextDeckIndex = activeDeckIndex === 0 ? 1 : 0;
            if (canCrossfade) {
              beginCrossfade(activeAudio, inactiveAudio, nextDeckIndex);
              setIsPlaying(true);
              setStreamStarted(true);
              syncTimelineFromAudio(inactiveAudio, true);
              return;
            }
            promoteDeck(nextDeckIndex);
            inactiveAudio.volume = muted() ? 0 : volume();
            setIsPlaying(true);
            setStreamStarted(true);
            syncTimelineFromAudio(inactiveAudio, true);
          })
          .catch(() => {
            inactiveAudio.pause();
            if (canCrossfade) {
              setIsPlaying(!activeAudio.paused);
              setStreamStarted(!activeAudio.paused);
              syncTimelineFromAudio(activeAudio);
              return;
            }
            activeAudio.pause();
            activeAudio.currentTime = 0;
            activeAudio.dataset.songId = song.id;
            activeAudio.src = nextRelativeUrl;
            activeAudio.preload = "auto";
            activeAudio.load();
            syncTimelineFromAudio(activeAudio, true);
            void activeAudio.play()
              .then(() => {
                setIsPlaying(true);
                setStreamStarted(true);
                syncTimelineFromAudio(activeAudio, true);
              })
              .catch(() => {
                setIsPlaying(false);
                setStreamStarted(false);
              });
          });
      } else if (!isSameSong) {
        promoteDeck(activeDeckIndex === 0 ? 1 : 0);
        activeAudio.pause();
      }
      return;
    }

    if (autoplay) {
      if (activeAudio.readyState < 2) {
        activeAudio.load();
      }
      void activeAudio.play().catch(() => {});
    } else if (!isSameSong && activeAudio.preload !== "auto") {
      activeAudio.preload = "auto";
    }
  };

  const primeSongAudio = (song) => {
    const activeAudio = getActiveAudio();
    if (!song || !activeAudio || isPlaying()) {
      return;
    }

    prefetchSongIds(getSongNeighborhoodIds(song));

    const version = encodeURIComponent(song.updatedAt || song.id);
    const nextRelativeUrl = `${song.audioUrl}?v=${version}`;
    const nextUrl = new URL(nextRelativeUrl, window.location.origin).href;
    if (activeAudio.src === nextUrl) {
      if (activeAudio.preload !== "auto") {
        activeAudio.preload = "auto";
      }
      return;
    }

    stopCrossfade();
    activeAudio.pause();
    activeAudio.dataset.songId = song.id;
    activeAudio.src = nextRelativeUrl;
    activeAudio.preload = "auto";
    activeAudio.load();
    syncTimelineFromAudio(activeAudio, true);
  };

  const moveSelection = (offset) => {
    const nextSong = pickRelativeSong(offset, selectedId(), { respectRandom: false });
    if (!nextSong) {
      return;
    }
    setSelectedId(nextSong.id);
  };

  const pickRelativeSong = (offset, baseId = selectedId(), options = {}) => {
    const { respectRandom = true } = options;
    const list = activeSongList();
    if (!list.length) {
      return null;
    }

    if (mainTab() === "radio") {
      const current = list.findIndex((song) => song.id === baseId);
      const nextIndex = current >= 0 ? (current + offset + list.length) % list.length : 0;
      return list[nextIndex] || null;
    }

    if (respectRandom && repeatMode() === "random") {
      const base = list.find((song) => song.id === baseId);
      const pool = list.filter((song) => song.id !== base?.id);
      return pool[Math.floor(Math.random() * pool.length)] || list[0];
    }

    const current = list.findIndex((song) => song.id === baseId);
    const from = current >= 0 ? current : 0;
    if (repeatMode() === "album") {
      const nextIndex = (from + offset + list.length) % list.length;
      return list[nextIndex] || null;
    }
    const next = Math.min(list.length - 1, Math.max(0, from + offset));
    return list[next];
  };

  const selectRelative = (offset, autoplay = false, baseId = selectedId(), options = {}) => {
    const nextSong = pickRelativeSong(offset, baseId, { respectRandom: autoplay });
    if (!nextSong) {
      return;
    }
    if (autoplay) {
      loadSong(nextSong, true, options);
      return;
    }
    setSelectedId(nextSong.id);
  };

  const adjustSeek = (deltaSeconds) => {
    if (radioPlaybackLocked()) {
      return;
    }
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

    if (mainTab() === "radio" && !currentRadioStation()) {
      startRadio();
      return;
    }

    if (!currentSong() && activeSongList()[0]) {
      loadSong(mainTab() === "radio" ? activeSongList()[0] : (selectedActiveSong() || selectedSong() || activeSongList()[0]), true);
      return;
    }

    if (activeAudio.paused) {
      if (selectedActiveSong() && selectedActiveSong()?.id !== currentTrackId()) {
        loadSong(selectedActiveSong(), true);
        return;
      }
      if (!activeAudio.src) {
        const fallbackSong = mainTab() === "radio"
          ? currentSong() || activeSongList()[0]
          : selectedActiveSong() || selectedSong() || currentSong() || activeSongList()[0];
        if (fallbackSong) {
          loadSong(fallbackSong, true);
          return;
        }
      }
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
    loadingTimer = setInterval(() => {
      setLoadingFrame((value) => value + 1);
    }, 280);

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
      if (appOffline()) {
        return;
      }
      const commandKey = event.ctrlKey || event.metaKey;
      const editable = isEditableTarget(event.target);

      if (event.key === "Escape" && showProfileMenu()) {
        event.preventDefault();
        setShowProfileMenu(false);
        return;
      }

      if (commandKey && event.key.toLowerCase() === "s") {
        event.preventDefault();
        focusSearch();
        return;
      }

      if (commandKey && event.key.toLowerCase() === "k") {
        event.preventDefault();
        focusSearch();
        return;
      }

      if (!editable && event.key === "/") {
        event.preventDefault();
        focusSearch();
        return;
      }

      if (!editable && event.key === "?") {
        event.preventDefault();
        setShowShortcutHelp((value) => !value);
        return;
      }

      if (event.key === "Escape") {
        if (showShortcutHelp()) {
          event.preventDefault();
          setShowShortcutHelp(false);
          return;
        }
        if (editable && event.target !== searchInputRef) {
          return;
        }
        if (document.activeElement === searchInputRef) {
          event.preventDefault();
          searchInputRef.blur();
          return;
        }
      }

      if (commandKey && ["1", "2", "3", "4", "5", "6"].includes(event.key)) {
        event.preventDefault();
        if (event.key === "1") activateMainTabShortcut("library");
        if (event.key === "2") activateMainTabShortcut("recents");
        if (event.key === "3") activateMainTabShortcut("favorites");
        if (event.key === "4") activateMainTabShortcut("playlists");
        if (event.key === "5" && radioEnabled()) activateMainTabShortcut("radio");
        if (event.key === "6") activateMainTabShortcut("admin");
        return;
      }

      if (editable) {
        return;
      }

      const isModifierSeek = commandKey;

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
        return;
      }

      if (event.key === "ArrowUp") {
        event.preventDefault();
        if (isModifierSeek) {
          adjustVolume(0.05);
          return;
        }
        setKeyboardNavigating(true);
        clearTimeout(keyboardNavTimer);
        keyboardNavTimer = setTimeout(() => setKeyboardNavigating(false), 180);
        selectRelative(-1, false);
        return;
      }

      if (event.key === "ArrowLeft" && isModifierSeek) {
        event.preventDefault();
        adjustSeek(-5);
        return;
      }

      if (event.key === "ArrowRight" && isModifierSeek) {
        event.preventDefault();
        adjustSeek(5);
        return;
      }

      if (event.key === "Enter") {
        event.preventDefault();
        loadSong(selectedActiveSong() || selectedSong() || activeSongList()[0], true);
        return;
      }

      if (event.key === " ") {
        event.preventDefault();
        if (!isPlaying()) {
          loadSong(selectedActiveSong() || selectedSong() || activeSongList()[0], true);
        } else {
          togglePlayback();
        }
        return;
      }

      if (event.key === "[") {
        event.preventDefault();
        if (!radioPlaybackLocked()) {
          selectRelative(-1, true, selectedId() || currentTrackId(), { allowCrossfade: true });
        }
        return;
      }

      if (event.key === "]") {
        event.preventDefault();
        if (!radioPlaybackLocked()) {
          selectRelative(1, true, selectedId() || currentTrackId(), { allowCrossfade: true });
        }
        return;
      }

      if (event.key.toLowerCase() === "m") {
        event.preventDefault();
        setMuted((value) => !value);
        return;
      }

      if (event.key.toLowerCase() === "t") {
        event.preventDefault();
        toggleThemePreference();
        return;
      }

      if (event.key.toLowerCase() === "l" && user() && currentSong()) {
        event.preventDefault();
        void toggleFavorite(currentSong().id);
        return;
      }

      if (event.key.toLowerCase() === "r") {
        event.preventDefault();
        activateMainTabShortcut("radio");
        startRadio();
        return;
      }
    };

    const onPointerDown = (event) => {
      if (!showProfileMenu()) {
        return;
      }
      const target = event.target;
      if (profileMenuRef?.contains(target) || profileMenuButtonRef?.contains(target)) {
        return;
      }
      setShowProfileMenu(false);
    };

    window.addEventListener("keydown", onKeyDown);
    window.addEventListener("pointerdown", onPointerDown);
    const onBrowserOnline = () => {
      void verifyAppOnline();
    };
    const onBrowserOffline = () => {
      healthFailCount = 3;
      markAppOffline("No internet connection detected.");
    };
    window.addEventListener("online", onBrowserOnline);
    window.addEventListener("offline", onBrowserOffline);
    removeKeydownListener = () => window.removeEventListener("keydown", onKeyDown);
    removePointerdownListener = () => window.removeEventListener("pointerdown", onPointerDown);
    removeOnlineListener = () => window.removeEventListener("online", onBrowserOnline);
    removeOfflineListener = () => window.removeEventListener("offline", onBrowserOffline);

    void verifyAppOnline();
    clearInterval(healthPollTimer);
    healthPollTimer = setInterval(() => {
      void verifyAppOnline();
    }, 30000);

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

      setLocalMode(Boolean(configPayload.localMode));
      setGoogleClientId(configPayload.googleClientId || "");
      setGeminiRadioEnabled(Boolean(configPayload.geminiRadioEnabled));
      setGeminiKeyCount(Number(configPayload.geminiKeyCount || 0));
      setSpotifyClientId(configPayload.spotifyClientId || "");
      setSpotifyRedirectUri(configPayload.spotifyRedirectUri || "");
      setSpotifyScopes(configPayload.spotifyScopes || "");
      setConfigReady(true);
      setStats(statsPayload);
      setSongs(initialSongs);
      setResults({ songs: initialSongs.slice(0, 200), albums: [], artists: [] });
      setSelectedId(initialSongs[0]?.id || "");
      setCurrentTrackId(initialSongs[0]?.id || "");
      worker.postMessage({ type: "index", payload: initialSongs });
      prefetchSongIds(initialSongs.slice(0, 12).map((song) => song.id));

      if (initialSongs[0] && getActiveAudio()) {
        const version = encodeURIComponent(initialSongs[0].updatedAt || initialSongs[0].id);
        getActiveAudio().src = `${initialSongs[0].audioUrl}?v=${version}`;
        getActiveAudio().preload = "auto";
        syncDeckVolumes();
        syncTimelineFromAudio(getActiveAudio(), true);
      }

      if (!Boolean(configPayload.localMode)) {
        await completeSpotifyAuthFromUrl();
      }
      if (Boolean(configPayload.localMode)) {
        await refreshDbSyncStatus();
        void refreshCacheStatus();
        clearInterval(cachePollTimer);
        cachePollTimer = setInterval(() => void refreshCacheStatus(), 60000);
      }
      await refreshAccountState();
      if (radioEnabled()) {
        void fetchRadioStations().catch(() => {});
      }
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
    playbackSpeed();
    syncDeckPlaybackSpeed();
  });

  createEffect(() => {
    document.documentElement.dataset.theme = theme();
  });

  createEffect(() => {
    if (!authEnabled() || !googleClientId()) {
      setGoogleReady(false);
      return;
    }
    if (window.google?.accounts?.id) {
      setGoogleReady(true);
      return;
    }
    void ensureGoogleScript()
      .then(() => setGoogleReady(true))
      .catch(() => setGoogleReady(false));
  });

  createEffect(() => {
    if (authEnabled()) {
      return;
    }
    setShowAuthPrompt(false);
    setShowProfileMenu(false);
    setAccountMessage("");
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
    if (user()?.is_admin && mainTab() === "admin") {
      void refreshAdminState().catch((error) => setAdminMessage(error?.message || "Unable to load admin panel"));
    }
  });

  createEffect(() => {
    clearInterval(adminRefreshTimer);
    if (user()?.is_admin && mainTab() === "admin") {
      adminRefreshTimer = setInterval(() => {
        void refreshAdminState().catch(() => {});
      }, 10000);
    }
  });

  createEffect(() => {
    clearInterval(dbSyncPollTimer);
    if (!localMode()) {
      setDbSyncState(null);
      return;
    }
    void refreshDbSyncStatus();
    dbSyncPollTimer = setInterval(() => {
      void refreshDbSyncStatus();
    }, 15000);
  });

  createEffect(() => {
    if (!user() && mainTab() === "favorites") {
      setMainTab("library");
    }
    if (!user()?.is_admin && mainTab() === "admin") {
      setMainTab("library");
    }
  });

  createEffect(() => {
    if (!authEnabled() || !googleClientId() || user() || !window.google?.accounts?.id || googleInitialized()) {
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
    if (!authEnabled() || !googleReady() || user() || !googleButtonRef || !window.google?.accounts?.id) {
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
    if (loading() || appOffline()) {
      return;
    }
    const song = selectedActiveSong() || selectedSong() || activeSongList()[0];
    if (!song) {
      return;
    }
    primeSongAudio(song);
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
    const listRect = listRef.getBoundingClientRect();
    const rowRect = row.getBoundingClientRect();
    const topPadding = 12;
    const bottomPadding = 12;

    if (rowRect.top < listRect.top + topPadding || rowRect.bottom > listRect.bottom - bottomPadding) {
      const nextScrollTop = rowRect.top < listRect.top + topPadding
        ? listRef.scrollTop - ((listRect.top + topPadding) - rowRect.top)
        : listRef.scrollTop + (rowRect.bottom - (listRect.bottom - bottomPadding));
      animateListScroll(Math.max(0, nextScrollTop));
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
    if (!radioLoading()) {
      void fetchRadioStations();
    }
  });

  createEffect(() => {
    clearInterval(radioSyncTimer);
    if (mainTab() !== "radio" || !currentRadioStation()) {
      return;
    }
    radioSyncTimer = setInterval(() => {
      const station = currentRadioStation();
      if (!station || !station.songIds?.length) {
        return;
      }
      const playback = resolveRadioStationPlayback(station);
      const sharedSongId = playback.currentSongId || "";
      if (!sharedSongId || currentTrackId() === sharedSongId) {
        return;
      }
      applyRadioStation(station.id, true);
    }, 5000);
  });

  createEffect(() => {
    if (!appOffline()) {
      return;
    }
    stopCrossfade();
    audioRefs.forEach((audio) => audio?.pause());
    setIsPlaying(false);
    setStreamStarted(false);
  });

  onCleanup(() => {
    clearTimeout(searchTimeout);
    clearTimeout(prefetchTimer);
    clearTimeout(keyboardNavTimer);
    clearInterval(adminRefreshTimer);
    clearInterval(loadingTimer);
    clearInterval(radioSyncTimer);
    clearInterval(healthPollTimer);
    clearInterval(dbSyncPollTimer);
    stopCrossfade();
    cancelAnimationFrame(scrollAnimationFrame);
    removeKeydownListener?.();
    removePointerdownListener?.();
    removeOnlineListener?.();
    removeOfflineListener?.();
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
    <main class={`relative flex h-dvh min-h-0 flex-col overflow-hidden bg-[var(--bg)] text-[var(--fg)] ${appOffline() ? "select-none opacity-60" : ""}`}>
      <Show when={appOffline()}>
        <div class="pointer-events-auto absolute inset-x-0 top-0 z-[80] border-b border-[var(--line)] bg-[var(--bg)]/95 px-6 py-3 backdrop-blur">
          <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--fg)]">Offline</div>
          <div class="mt-1 text-sm text-[var(--soft)]">{offlineMessage() || "Backend is not responding. Docker may need a restart."}</div>
          <button
            type="button"
            onClick={() => { healthFailCount = 0; markAppOnline(); void verifyAppOnline(); }}
            class="mt-2 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--fg)] underline transition hover:text-[var(--soft)]"
          >
            Retry now
          </button>
        </div>
      </Show>
      <Show when={!appOffline() && localMode() && cacheFull()}>
        <div class="border-b border-[var(--line)] bg-[var(--bg)] px-6 py-2">
          <div class="flex items-center justify-between gap-4">
            <div class="text-sm text-[var(--soft)]">
              Cache is {cachePercent()}% full ({((cacheStatus()?.usageBytes || 0) / (1024 * 1024 * 1024)).toFixed(1)} / {cacheStatus()?.limitGb || 0} GB). Old songs will be removed automatically.
            </div>
            <button
              type="button"
              onClick={() => void trimCache(false)}
              disabled={cacheTrimming()}
              class="shrink-0 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--fg)] transition hover:text-[var(--soft)] disabled:opacity-50"
            >
              {cacheTrimming() ? "Clearing..." : "Clear now"}
            </button>
          </div>
        </div>
      </Show>
      <header class="flex min-w-0 flex-wrap items-center gap-3 border-b border-[var(--line)] px-6 py-4 sm:flex-nowrap sm:justify-between">
        <span class="group relative inline-flex shrink-0 items-center gap-3">
          <BrandIcon />
          <span class="font-mono text-[11px] uppercase tracking-[0.32em] text-[var(--brand)]">isaibox</span>
          <span class="pointer-events-none absolute left-0 top-full z-20 mt-3 min-w-[220px] border border-[var(--line)] bg-[var(--bg)] px-3 py-3 opacity-0 shadow-lg transition-opacity duration-100 group-hover:opacity-100 group-focus-within:opacity-100">
            <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Library status</div>
            <div class="mt-2 text-sm text-[var(--fg)]">
              <Show when={stats()} fallback={"Loading library..."}>
                {(libraryStats) => (
                  <div class="space-y-1">
                    <div>{libraryStats().songs.toLocaleString()} tracks indexed</div>
                    <div class="text-[var(--soft)]">Last update: {formatUpdatedAt(libraryStats().latestUpdatedAt)}</div>
                  </div>
                )}
              </Show>
            </div>
          </span>
        </span>
        <div class="ml-auto flex min-w-0 items-center justify-end gap-2 md:gap-3">
          <div
            ref={(el) => {
              googleButtonRef = el;
            }}
            aria-hidden="true"
            class="pointer-events-none absolute left-[-9999px] top-0 opacity-0"
          />
          <Show when={localMode()}>
            <div class="flex min-w-0 items-center gap-2">
              <button
                type="button"
                onClick={() => setShowSettings(!showSettings())}
                class={`rounded-full border px-3 py-2 font-mono text-[10px] uppercase tracking-[0.22em] transition ${
                  showSettings() ? "border-[var(--fg)] text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)]"
                }`}
              >
                Settings
              </button>
              <Show when={cacheNearFull() && !cacheFull()}>
                <span class="rounded-full border border-yellow-500/40 px-3 py-2 font-mono text-[10px] uppercase tracking-[0.18em] text-yellow-500">
                  Cache {cachePercent()}%
                </span>
              </Show>
              <Show when={localDbSyncLabel()}>
                <span
                  class={`max-w-[220px] truncate rounded-full border px-3 py-2 font-mono text-[10px] uppercase tracking-[0.18em] ${localDbSyncTone()}`}
                  title={dbSyncState()?.message || localDbSyncLabel()}
                >
                  {localDbSyncLabel()}
                </span>
              </Show>
              <Show when={dbSyncState()?.status === "error" && dbSyncState()?.githubIssuesUrl}>
                <a
                  href={dbSyncState().githubIssuesUrl}
                  target="_blank"
                  rel="noreferrer"
                  class="text-xs text-[var(--soft)] underline decoration-[var(--line)] underline-offset-4 transition hover:text-[var(--fg)]"
                >
                  Raise issue
                </a>
              </Show>
            </div>
          </Show>
          <Show when={authEnabled()}>
          <Show
            when={user()}
            fallback={
              <div
                class="relative"
                onMouseEnter={() => setShowProfileMenu(true)}
                onMouseLeave={() => setShowProfileMenu(false)}
              >
                <button
                  type="button"
                  ref={(el) => {
                    profileMenuButtonRef = el;
                  }}
                  onClick={() => setShowProfileMenu((value) => !value)}
                  aria-label="Open account menu"
                  class={`flex h-9 items-center gap-2 rounded-full border px-2.5 text-[var(--soft)] transition ${
                    showProfileMenu() ? "border-[var(--fg)] bg-[var(--hover)] text-[var(--fg)]" : "border-[var(--line)] hover:border-[var(--fg)] hover:text-[var(--fg)]"
                  }`}
                >
                  <span class="flex h-7 w-7 items-center justify-center rounded-full border border-current">
                    <UserIcon />
                  </span>
                  <span class="hidden text-sm md:block">Account</span>
                  <ChevronDownIcon />
                </button>
                <Show when={showProfileMenu()}>
                  <div
                    ref={(el) => {
                      profileMenuRef = el;
                    }}
                    class="absolute right-0 top-full z-30 mt-3 w-[250px] border border-[var(--line)] bg-[var(--bg)] p-3 shadow-lg"
                  >
                    <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Login</div>
                    <button
                      type="button"
                      onClick={() => {
                        setShowProfileMenu(false);
                        beginGoogleLogin();
                      }}
                      class="mt-3 flex w-full items-center justify-between border border-[var(--line)] px-3 py-2 text-left transition hover:border-[var(--fg)] disabled:cursor-not-allowed disabled:opacity-40"
                      disabled={!googleReady()}
                    >
                      <span class="text-sm text-[var(--fg)]">Sign in with Google</span>
                      <UserIcon />
                    </button>
                    <div class="mt-4 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Theme</div>
                    <div class="mt-2 grid grid-cols-3 gap-2">
                      <button type="button" onClick={() => setThemePreferenceChoice("light")} class={`border px-2 py-2 font-mono text-[10px] uppercase tracking-[0.16em] transition ${themePreference() === "light" ? "border-[var(--fg)] bg-[var(--hover)] text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>Light</button>
                      <button type="button" onClick={() => setThemePreferenceChoice("system")} class={`border px-2 py-2 font-mono text-[10px] uppercase tracking-[0.16em] transition ${themePreference() === "system" ? "border-[var(--fg)] bg-[var(--hover)] text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>System</button>
                      <button type="button" onClick={() => setThemePreferenceChoice("dark")} class={`border px-2 py-2 font-mono text-[10px] uppercase tracking-[0.16em] transition ${themePreference() === "dark" ? "border-[var(--fg)] bg-[var(--hover)] text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>Dark</button>
                    </div>
                  </div>
                </Show>
              </div>
            }
          >
            {(account) => (
              <div class="flex min-w-0 items-center gap-2 md:gap-3">
                <Show when={account().is_admin}>
                  <span class="rounded-full border border-[var(--line)] px-3 py-2 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)]">
                    Admin
                  </span>
                </Show>
                <div
                  class="relative"
                  onMouseEnter={() => setShowProfileMenu(true)}
                  onMouseLeave={() => setShowProfileMenu(false)}
                >
                  <button
                    type="button"
                    ref={(el) => {
                      profileMenuButtonRef = el;
                    }}
                    onClick={() => setShowProfileMenu((value) => !value)}
                    aria-label="Open profile menu"
                    class={`flex h-9 items-center gap-2 rounded-full border px-2.5 text-[var(--soft)] transition ${
                      showProfileMenu() ? "border-[var(--fg)] bg-[var(--hover)] text-[var(--fg)]" : "border-[var(--line)] hover:border-[var(--fg)] hover:text-[var(--fg)]"
                    }`}
                  >
                    <span class="flex h-7 w-7 items-center justify-center rounded-full border border-current font-mono text-[11px] uppercase tracking-[0.16em]">
                      {getInitials(account().name, account().email)}
                    </span>
                    <span class="hidden max-w-[120px] truncate text-sm md:block lg:max-w-[160px]">{account().name || account().email}</span>
                    <ChevronDownIcon />
                  </button>
                  <Show when={showProfileMenu()}>
                    <div
                      ref={(el) => {
                        profileMenuRef = el;
                      }}
                      class="absolute right-0 top-full z-30 mt-3 w-[250px] border border-[var(--line)] bg-[var(--bg)] p-3 shadow-lg"
                    >
                      <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Profile settings</div>
                      <div class="mt-3">
                        <div class="truncate text-sm text-[var(--fg)]">{account().name || "Account"}</div>
                        <div class="mt-1 truncate font-mono text-[10px] text-[var(--soft)]">{account().email}</div>
                      </div>
                      <div class="mt-4 space-y-2">
                        <div>
                          <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Theme</div>
                          <div class="mt-2 grid grid-cols-3 gap-2">
                            <button type="button" onClick={() => setThemePreferenceChoice("light")} class={`border px-2 py-2 font-mono text-[10px] uppercase tracking-[0.16em] transition ${themePreference() === "light" ? "border-[var(--fg)] bg-[var(--hover)] text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>Light</button>
                            <button type="button" onClick={() => setThemePreferenceChoice("system")} class={`border px-2 py-2 font-mono text-[10px] uppercase tracking-[0.16em] transition ${themePreference() === "system" ? "border-[var(--fg)] bg-[var(--hover)] text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>System</button>
                            <button type="button" onClick={() => setThemePreferenceChoice("dark")} class={`border px-2 py-2 font-mono text-[10px] uppercase tracking-[0.16em] transition ${themePreference() === "dark" ? "border-[var(--fg)] bg-[var(--hover)] text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>Dark</button>
                          </div>
                        </div>
                        <button
                          type="button"
                          onClick={() => {
                            setShowProfileMenu(false);
                            void logout();
                          }}
                          class="flex w-full items-center justify-between border border-[var(--line)] px-3 py-2 text-left transition hover:border-[var(--fg)]"
                        >
                          <span class="text-sm text-[var(--fg)]">Logout</span>
                          <LogoutIcon />
                        </button>
                      </div>
                    </div>
                  </Show>
                </div>
              </div>
            )}
          </Show>
          </Show>
        </div>
      </header>

      <Show when={showSettings() && localMode()}>
        <section class="border-b border-[var(--line)] bg-[var(--panel)] px-6 py-4">
          <div class="flex items-center justify-between gap-4">
            <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Settings</div>
            <button
              type="button"
              onClick={() => setShowSettings(false)}
              class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
            >
              Close
            </button>
          </div>
          <div class="mt-4 space-y-4">
            <div class="border border-[var(--line)] p-4">
              <div class="flex items-center justify-between gap-4">
                <div>
                  <div class="text-sm font-semibold">Audio cache</div>
                  <div class="mt-1 text-sm text-[var(--soft)]">
                    <Show when={cacheStatus()} fallback="Loading cache info...">
                      {(() => {
                        const usageMb = () => ((cacheStatus()?.usageBytes || 0) / (1024 * 1024)).toFixed(0);
                        const usageGb = () => ((cacheStatus()?.usageBytes || 0) / (1024 * 1024 * 1024)).toFixed(2);
                        const limitGb = () => cacheStatus()?.limitGb || 0;
                        return (
                          <span>
                            {Number(usageMb()) > 1024 ? `${usageGb()} GB` : `${usageMb()} MB`} used of {limitGb()} GB limit ({cachePercent()}%)
                          </span>
                        );
                      })()}
                    </Show>
                  </div>
                  <Show when={cacheStatus()}>
                    <div class="mt-2 h-2 w-full max-w-[300px] overflow-hidden rounded-full bg-[var(--line)]">
                      <div
                        class={`h-full rounded-full transition-all ${cacheFull() ? "bg-red-500" : cacheNearFull() ? "bg-yellow-500" : "bg-green-500"}`}
                        style={`width: ${cachePercent()}%`}
                      />
                    </div>
                  </Show>
                </div>
                <div class="flex flex-wrap items-center gap-3">
                  <button
                    type="button"
                    onClick={() => void trimCache(false)}
                    disabled={cacheTrimming()}
                    class="border border-[var(--line)] px-3 py-2 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)] disabled:opacity-50"
                  >
                    {cacheTrimming() ? "Clearing..." : "Smart clear"}
                  </button>
                  <button
                    type="button"
                    onClick={() => void trimCache(true)}
                    disabled={cacheTrimming()}
                    class="border border-[var(--line)] px-3 py-2 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)] disabled:opacity-50"
                  >
                    {cacheTrimming() ? "Clearing..." : "Clear all"}
                  </button>
                </div>
              </div>
              <Show when={cacheMessage()}>
                <div class="mt-3 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)]">{cacheMessage()}</div>
              </Show>
              <div class="mt-3 text-xs text-[var(--muted)]">
                Smart clear removes oldest cached songs to stay within the limit. Clear all removes every cached file.
              </div>
            </div>
          </div>
        </section>
      </Show>

      <section class="border-b border-[var(--line)] px-6 py-3">
        <div class="flex flex-wrap items-center justify-between gap-4">
          <div class="flex flex-wrap items-center gap-5 font-mono text-[10px] uppercase tracking-[0.22em]">
            <button type="button" onClick={() => setMainBrowseTab("library")} class={`px-1 py-1 ${mainTab() === "library" ? "text-[var(--fg)]" : "text-[var(--soft)]"}`}>
              Library
            </button>
            <button type="button" onClick={() => setMainBrowseTab("recents")} class={`px-1 py-1 ${mainTab() === "recents" ? "text-[var(--fg)]" : "text-[var(--soft)]"}`}>
              Recents
            </button>
            <Show when={libraryProfileEnabled()}>
              <button type="button" onClick={() => setMainBrowseTab("favorites")} class={`px-1 py-1 ${mainTab() === "favorites" ? "text-[var(--fg)]" : "text-[var(--soft)]"}`}>
                Favorites {favoriteSongs().length ? `(${favoriteSongs().length})` : ""}
              </button>
            </Show>
            <Show when={radioEnabled()}>
              <button type="button" onClick={() => setMainBrowseTab("radio")} class={`px-1 py-1 ${mainTab() === "radio" ? "text-[var(--fg)]" : "text-[var(--soft)]"}`}>
                Radio
              </button>
            </Show>
            <Show when={localMode()}>
              <button type="button" onClick={() => setMainBrowseTab("playlists")} class={`px-1 py-1 ${mainTab() === "playlists" ? "text-[var(--fg)]" : "text-[var(--soft)]"}`}>
                Playlists
              </button>
            </Show>
            <Show when={authEnabled() && user()?.is_admin}>
              <button type="button" onClick={() => setMainBrowseTab("admin")} class={`px-1 py-1 ${mainTab() === "admin" ? "text-[var(--fg)]" : "text-[var(--soft)]"}`}>
                Admin
              </button>
            </Show>
          </div>
          <Show when={mainTab() === "library"}>
            <div class="flex min-w-[280px] flex-1 items-center justify-end gap-3">
              <div class="flex w-full max-w-[560px] items-center gap-3 border border-[var(--line)] px-3 py-2">
                <span class="font-mono text-sm text-[var(--soft)]">/</span>
                <input
                  ref={(el) => {
                    searchInputRef = el;
                  }}
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
                  class="shrink-0 font-mono text-xs uppercase tracking-[0.2em] text-[var(--muted)] transition hover:text-[var(--fg)]"
                >
                  Clear
                </button>
              </div>
            </div>
          </Show>
        </div>
      </section>

      <Show when={user()?.is_admin && mainTab() === "admin"}>
        <section class="border-b border-[var(--line-soft)] px-6 py-4">
          <div class="flex flex-wrap items-center justify-between gap-3 border-b border-[var(--line-soft)] pb-3">
            <div>
              <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Admin workspace</div>
              <div class="mt-1 text-sm text-[var(--soft)]">Manage playlists, users, and scraping from one place.</div>
            </div>
            <button
              type="button"
              onClick={() => void refreshAdminState().catch((error) => setAdminMessage(error?.message || "Unable to refresh admin state"))}
              class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
            >
              Refresh
            </button>
          </div>

          <div class="mt-4 flex flex-wrap items-center gap-4 font-mono text-[10px] uppercase tracking-[0.22em]">
            <button type="button" onClick={() => setAdminTab("playlists")} class={adminTab() === "playlists" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>Playlists</button>
            <button type="button" onClick={() => setAdminTab("users")} class={adminTab() === "users" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>Users</button>
            <button type="button" onClick={() => setAdminTab("airflow")} class={adminTab() === "airflow" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>Airflow</button>
          </div>

          <Show when={adminTab() === "playlists"}>
            <div class="mt-4 space-y-4">
              <div class="flex flex-wrap items-center gap-3 border border-[var(--line)] p-4">
                <span class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Global playlists</span>
                <input
                  value={globalPlaylistNameInput()}
                  onInput={(event) => setGlobalPlaylistNameInput(event.currentTarget.value)}
                  placeholder="New global playlist"
                  class="min-w-[180px] bg-transparent font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                />
                <button
                  type="button"
                  onClick={() => void createGlobalPlaylist()}
                  class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                >
                  Create global
                </button>
                <Show when={geminiRadioEnabled()}>
                  <button
                    type="button"
                    onClick={() => void generateAiPlaylists()}
                    class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                  >
                    AI global
                  </button>
                </Show>
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
                    Add current
                  </button>
                </Show>
              </div>

              <div class="border border-[var(--line)] p-4">
                <div class="mb-3 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Radio to playlist</div>
                <div class="flex flex-wrap items-center gap-3">
                  <select
                    value={selectedRadioStationId()}
                    onChange={(event) => setSelectedRadioStationId(event.currentTarget.value)}
                    class="bg-transparent font-mono text-xs text-[var(--fg)] outline-none"
                  >
                    <For each={radioStations()}>
                      {(station) => <option value={station.id}>{station.name}</option>}
                    </For>
                  </select>
                  <select
                    value={radioSaveMode()}
                    onChange={(event) => setRadioSaveMode(event.currentTarget.value)}
                    class="bg-transparent font-mono text-xs text-[var(--fg)] outline-none"
                  >
                    <option value="overwrite">Overwrite existing</option>
                    <option value="new">Save as new</option>
                    <option value="upsert">Upsert station playlist</option>
                  </select>
                  <Show when={radioSaveMode() === "overwrite"}>
                    <select
                      value={selectedGlobalPlaylistTarget()}
                      onChange={(event) => setSelectedGlobalPlaylistTarget(event.currentTarget.value)}
                      class="bg-transparent font-mono text-xs text-[var(--fg)] outline-none"
                    >
                      <For each={globalPlaylists()}>
                        {(playlist) => <option value={playlist.id}>{playlist.name}</option>}
                      </For>
                    </select>
                  </Show>
                  <input
                    value={radioSaveName()}
                    onInput={(event) => setRadioSaveName(event.currentTarget.value)}
                    placeholder="Playlist name"
                    class="min-w-[180px] bg-transparent font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                  />
                  <button
                    type="button"
                    onClick={() => void saveRadioStationWithMode()}
                    class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                  >
                    Save radio
                  </button>
                </div>
              </div>

              <Show when={globalPlaylists().length > 0}>
                <div class="border border-[var(--line)] p-4">
                  <div class="mb-3 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Browse global playlists</div>
                  <div class="flex flex-wrap gap-2">
                    <For each={globalPlaylists()}>
                      {(playlist) => (
                        <button
                          type="button"
                          onClick={() => void openGlobalPlaylist(playlist.id)}
                          class={`border px-3 py-2 text-left transition ${
                            globalPlaylistDetail()?.id === playlist.id
                              ? "border-[var(--fg)] bg-[var(--hover)]"
                              : "border-[var(--line)] hover:border-[var(--fg)]"
                          }`}
                        >
                          <div class="text-sm">{playlist.name}</div>
                          <div class="font-mono text-[10px] text-[var(--soft)]">{playlist.trackCount} tracks · {playlist.source}</div>
                        </button>
                      )}
                    </For>
                  </div>
                  <Show when={globalPlaylistDetail()}>
                    {(playlist) => (
                      <div class="mt-4 border border-[var(--line-soft)] p-3">
                        <div class="mb-2 flex items-center justify-between gap-3">
                          <div>
                            <div class="text-sm">{playlist().name}</div>
                            <div class="font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)]">{playlist().source || "manual"}</div>
                          </div>
                          <div class="flex flex-wrap items-center gap-2">
                            <input
                              value={globalPlaylistNameEdit()}
                              onInput={(event) => setGlobalPlaylistNameEdit(event.currentTarget.value)}
                              class="min-w-[180px] bg-transparent font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                            />
                            <button
                              type="button"
                              onClick={() => void renameGlobalPlaylist()}
                              class="font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                            >
                              Rename
                            </button>
                          </div>
                        </div>
                        <div class="space-y-2">
                          <For each={playlist().tracks || []}>
                            {(track, index) => (
                              <div class="flex items-center justify-between gap-4 border-b border-[var(--line-soft)] pb-2 text-sm last:border-b-0 last:pb-0">
                                <button type="button" onClick={() => loadSong(track, true)} class="min-w-0 text-left">
                                  <span class="font-mono text-[10px] text-[var(--soft)]">{String(index() + 1).padStart(2, "0")}</span>
                                  <span class="ml-3 truncate">{track.track}</span>
                                </button>
                                <div class="flex items-center gap-3">
                                  <button
                                    type="button"
                                    onClick={() => void moveGlobalPlaylistSong(track.id, -1)}
                                    class="font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                                  >
                                    Up
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => void moveGlobalPlaylistSong(track.id, 1)}
                                    class="font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                                  >
                                    Down
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => void removeSongFromPlaylist(playlist().id, track.id)}
                                    class="font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                                  >
                                    Remove
                                  </button>
                                </div>
                              </div>
                            )}
                          </For>
                        </div>
                      </div>
                    )}
                  </Show>
                </div>
              </Show>
            </div>
          </Show>

          <Show when={adminTab() === "users"}>
            <div class="mt-4 overflow-x-auto border border-[var(--line)] p-4">
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
          </Show>

          <Show when={adminTab() === "airflow"}>
            <div class="mt-4 space-y-4">
              <div class="flex flex-wrap items-center gap-3 border border-[var(--line)] p-4">
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
              </div>

              <Show when={airflowStatus()}>
                {(status) => (
                  <div class="grid gap-3 md:grid-cols-4">
                    <div class="border border-[var(--line)] p-4">
                      <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Webserver</div>
                      <div class="mt-2 text-sm">{status().webserverRunning ? "Running" : "Stopped"}</div>
                    </div>
                    <div class="border border-[var(--line)] p-4">
                      <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Latest run</div>
                      <div class="mt-2 text-sm">{status().latestRun?.status || "unknown"}</div>
                    </div>
                    <div class="border border-[var(--line)] p-4">
                      <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Songs total</div>
                      <div class="mt-2 text-sm">{status().latestRun?.songsTotal || 0}</div>
                    </div>
                    <div class="border border-[var(--line)] p-4">
                      <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">DAG CLI</div>
                      <div class="mt-2 text-sm">{status().dagsOk ? "Healthy" : "Error"}</div>
                    </div>
                  </div>
                )}
              </Show>

              <Show when={airflowStatus()?.recentRuns?.length}>
                <div class="overflow-x-auto border border-[var(--line)] p-4">
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
            </div>
          </Show>

          <Show when={adminMessage()}>
            <div class="mt-4 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)]">{adminMessage()}</div>
          </Show>
        </section>
      </Show>

      <section class="flex min-h-0 flex-1 flex-col overflow-hidden">
        <Show when={showShortcutHelp()}>
          <div class="absolute inset-0 z-40 bg-black/30">
            <button
              type="button"
              aria-label="Close shortcuts"
              class="absolute inset-0 h-full w-full cursor-default"
              onClick={() => setShowShortcutHelp(false)}
            />
            <div class="absolute bottom-0 left-0 top-0 w-[min(92vw,360px)] border-r border-[var(--line)] bg-[var(--bg)] p-5 shadow-2xl">
              <div class="flex items-start justify-between gap-4">
                <div>
                  <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Keyboard shortcuts</div>
                  <div class="mt-2 text-sm text-[var(--soft)]">Global navigation, player control, and radio-safe shortcuts.</div>
                </div>
                <button
                  type="button"
                  onClick={() => setShowShortcutHelp(false)}
                  class="font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                >
                  Close
                </button>
              </div>
              <div class="mt-5 space-y-5 overflow-y-auto pb-8 pr-2 text-sm">
                <div class="space-y-3">
                  <div class="flex items-center justify-between gap-4"><span>Focus search</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl+S or /</span></div>
                  <div class="flex items-center justify-between gap-4"><span>Show shortcuts</span><span class="font-mono text-[11px] text-[var(--soft)]">?</span></div>
                  <div class="flex items-center justify-between gap-4"><span>Library</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl+1</span></div>
                  <div class="flex items-center justify-between gap-4"><span>Recents</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl+2</span></div>
                  <Show when={libraryProfileEnabled()}>
                    <div class="flex items-center justify-between gap-4"><span>Favorites</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl+3</span></div>
                  </Show>
                  <Show when={localMode()}>
                    <div class="flex items-center justify-between gap-4"><span>Playlists</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl+4</span></div>
                  </Show>
                  <div class="flex items-center justify-between gap-4"><span>Radio</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl+5 or R</span></div>
                  <Show when={authEnabled() && user()?.is_admin}>
                    <div class="flex items-center justify-between gap-4"><span>Admin</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl+6</span></div>
                  </Show>
                </div>
                <div class="space-y-3">
                  <div class="flex items-center justify-between gap-4"><span>Play / pause</span><span class="font-mono text-[11px] text-[var(--soft)]">Space</span></div>
                  <div class="flex items-center justify-between gap-4"><span>Play selected</span><span class="font-mono text-[11px] text-[var(--soft)]">Enter</span></div>
                  <div class="flex items-center justify-between gap-4"><span>Move selection</span><span class="font-mono text-[11px] text-[var(--soft)]">Up / Down</span></div>
                  <div class="flex items-center justify-between gap-4"><span>Seek</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl + Left / Right</span></div>
                  <div class="flex items-center justify-between gap-4"><span>Volume</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl + Up / Down</span></div>
                  <div class="flex items-center justify-between gap-4"><span>Prev / next song</span><span class="font-mono text-[11px] text-[var(--soft)]">[ and ]</span></div>
                  <Show when={libraryProfileEnabled()}>
                    <div class="flex items-center justify-between gap-4"><span>Favorite current</span><span class="font-mono text-[11px] text-[var(--soft)]">L</span></div>
                  </Show>
                  <div class="flex items-center justify-between gap-4"><span>Mute</span><span class="font-mono text-[11px] text-[var(--soft)]">M</span></div>
                  <div class="flex items-center justify-between gap-4"><span>Theme</span><span class="font-mono text-[11px] text-[var(--soft)]">T</span></div>
                  <div class="flex items-center justify-between gap-4"><span>Close panels</span><span class="font-mono text-[11px] text-[var(--soft)]">Esc</span></div>
                </div>
              </div>
            </div>
          </div>
        </Show>
        <Show when={mainTab() !== "library" && mainTab() !== "admin" && mainTab() !== "playlists"}>
          <section class="border-b border-[var(--line-soft)] px-6 py-4">
            <div class="flex flex-wrap items-center justify-between gap-3">
              <div>
                <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">
                  {mainTab() === "recents" ? "Recent plays" : mainTab() === "favorites" ? "Favorites" : "Radio station"}
                </div>
                <Show when={mainTab() === "radio"}>
                  <div class="mt-1 text-sm text-[var(--soft)]">
                    {currentRadioStation()?.blurb || "Gemini-sorted stations with looping 100-song queues."}
                  </div>
                </Show>
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
        </section>
        </Show>
        <Show when={mainTab() === "radio"}>
          <section class="min-h-0 flex-1 overflow-y-auto px-6 py-5">
            <div class="mx-auto flex w-full max-w-6xl flex-col gap-5 pb-8">
              <div class="flex flex-wrap items-center gap-3 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)]">
                <span>{radioLoading() ? "Building stations..." : `${filteredRadioStations().length} of ${radioStations().length} stations`}</span>
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

              <Show when={currentSong()}>
                {(song) => (
                  <div class="grid gap-4 border border-[var(--line)] bg-[var(--panel)] p-4 md:grid-cols-[1.1fr_0.9fr]">
                    <div>
                      <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Now playing</div>
                      <div class="mt-3 text-xl font-semibold">{song().track}</div>
                      <div class="mt-2 text-sm text-[var(--soft)]">
                        {song().singers || "Unknown singers"}
                      </div>
                      <div class="mt-4 flex flex-wrap gap-3 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--muted)]">
                        <span>{song().movie || "Single"}</span>
                        <span>{song().musicDirector || "Unknown composer"}</span>
                        <span>{song().year || "-"}</span>
                        <Show when={isPlaying() && streamStarted()}>
                          <span>Live</span>
                        </Show>
                      </div>
                    </div>
                    <div class="border border-[var(--line-soft)] p-4">
                      <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Current station</div>
                      <Show when={currentRadioStation()} fallback={<div class="mt-3 text-sm text-[var(--soft)]">Pick a station to start the loop.</div>}>
                        {(station) => (
                          <>
                            <div class="mt-3 text-base font-semibold">{station().name}</div>
                            <div class="mt-2 text-sm text-[var(--soft)]">{station().blurb}</div>
                            <div class="mt-4 flex flex-wrap gap-3 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--muted)]">
                              <span>{station().yearStart} - {station().yearEnd}</span>
                              <span>{station().trackCount} tracks looping</span>
                            </div>
                          </>
                        )}
                      </Show>
                    </div>
                  </div>
                )}
              </Show>

              <Show when={!currentSong()}>
                <div class="border border-[var(--line)] bg-[var(--panel)] p-5 text-sm text-[var(--soft)]">
                  Start a station to begin playback. The radio queue stays hidden and loops in the background.
                </div>
              </Show>

              <div class="border border-[var(--line)] bg-[var(--panel)] p-3">
                <input
                  value={radioSearchQuery()}
                  onInput={(event) => setRadioSearchQuery(event.currentTarget.value)}
                  placeholder="Search stations"
                  class="w-full bg-transparent px-1 py-2 font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                />
              </div>

              <div class="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                <For each={filteredRadioStations()}>
                  {(station) => (
                    <button
                      type="button"
                      onClick={() => applyRadioStation(station.id, true)}
                      class={`border p-4 text-left transition ${
                        selectedRadioStationId() === station.id
                          ? "border-[var(--fg)] bg-[var(--hover)]"
                          : "border-[var(--line)] hover:border-[var(--fg)] hover:bg-[var(--hover)]"
                      }`}
                    >
                      <div class="text-base font-semibold">{station.name}</div>
                      <div class="mt-2 line-clamp-2 text-sm text-[var(--soft)]">{station.blurb}</div>
                      <div class="mt-4 flex flex-wrap gap-3 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--muted)]">
                        <span>{station.yearStart} - {station.yearEnd}</span>
                        <span>{station.trackCount} tracks</span>
                      </div>
                    </button>
                  )}
                </For>
              </div>
              <Show when={!radioLoading() && filteredRadioStations().length === 0}>
                <div class="border border-[var(--line)] bg-[var(--panel)] p-5 text-sm text-[var(--soft)]">
                  No radio stations match that search.
                </div>
              </Show>
            </div>
          </section>
        </Show>
        <Show when={mainTab() === "playlists"}>
          {(() => {
            const userPlaylistIds = createMemo(() => new Set(playlists().map((p) => p.id)));
            const myPlaylistDetail = createMemo(() => {
              const detail = globalPlaylistDetail();
              if (!detail) return null;
              return userPlaylistIds().has(detail.id) ? detail : null;
            });
            return (
              <>
              <section class="border-b border-[var(--line-soft)] px-6 py-4">
                <div class="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">My playlists</div>
                    <div class="mt-1 text-sm text-[var(--soft)]">Create and manage your playlists.</div>
                  </div>
                  <div class="flex flex-wrap items-center gap-3 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)]">
                    <span>{playlists().length} playlists</span>
                  </div>
                </div>
              </section>
              <section class="min-h-0 flex-1 overflow-hidden px-6 py-4">
                <div class="grid h-full min-h-0 gap-4 xl:grid-cols-[280px_minmax(0,1fr)]">
                  <aside class="min-h-0 overflow-y-auto border border-[var(--line)] bg-[var(--panel)] p-4">
                    <div class="flex items-center gap-2">
                      <input
                        value={playlistNameInput()}
                        onInput={(event) => setPlaylistNameInput(event.currentTarget.value)}
                        onKeyDown={(event) => { if (event.key === "Enter") void createPlaylist(); }}
                        placeholder="New playlist name"
                        class="min-w-0 flex-1 border border-[var(--line)] bg-transparent px-3 py-2 font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                      />
                      <button
                        type="button"
                        onClick={() => void createPlaylist()}
                        class="border border-[var(--line)] px-3 py-2 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)]"
                      >
                        +
                      </button>
                    </div>

                    <Show when={playlists().length > 3}>
                      <div class="mt-3">
                        <input
                          value={playlistSearchQuery()}
                          onInput={(event) => setPlaylistSearchQuery(event.currentTarget.value)}
                          placeholder="Search"
                          class="w-full border border-[var(--line)] bg-transparent px-3 py-2 font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                        />
                      </div>
                    </Show>

                    <div class="mt-4 space-y-1">
                      <For each={filteredUserPlaylists()}>
                        {(playlist) => (
                          <button
                            type="button"
                            onClick={() => void openGlobalPlaylist(playlist.id)}
                            class={`flex w-full items-center justify-between gap-2 border px-3 py-3 text-left transition ${
                              myPlaylistDetail()?.id === playlist.id
                                ? "border-[var(--fg)] bg-[var(--hover)]"
                                : "border-[var(--line)] hover:border-[var(--fg)]"
                            }`}
                          >
                            <span class="min-w-0 truncate text-sm">{playlist.name}</span>
                            <span class="shrink-0 font-mono text-[10px] text-[var(--soft)]">{playlist.trackCount}</span>
                          </button>
                        )}
                      </For>

                      <Show when={normalizedPlaylistSearch() && filteredUserPlaylists().length === 0}>
                        <div class="px-1 py-3 text-sm text-[var(--soft)]">No match.</div>
                      </Show>

                      <Show when={!normalizedPlaylistSearch() && playlists().length === 0}>
                        <div class="px-1 py-3 text-sm text-[var(--soft)]">No playlists yet.</div>
                      </Show>
                    </div>
                  </aside>

                  <div class="flex min-h-0 flex-col overflow-hidden border border-[var(--line)] bg-[var(--panel)]">
                    <Show when={playlistDetailLoading() && !myPlaylistDetail()}>
                      <div class="flex min-h-0 flex-1 items-center justify-center px-6 text-center">
                        <div>
                          <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Playlist</div>
                          <div class="mt-3 text-sm text-[var(--soft)]">Loading{loadingDots()}</div>
                        </div>
                      </div>
                    </Show>
                    <Show when={myPlaylistDetail()}>
                      {(playlist) => (
                        <>
                          <section class="border-b border-[var(--line-soft)] px-4 py-4">
                            <div class="flex items-center justify-between gap-4">
                              <div class="min-w-0">
                                <div class="mt-1 text-lg font-semibold">{playlist().name}</div>
                                <div class="mt-1 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)]">
                                  {(playlist().tracks || []).length} tracks
                                </div>
                              </div>
                              <div class="flex flex-wrap items-center gap-3 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)]">
                                <input
                                  value={globalPlaylistNameEdit()}
                                  onInput={(event) => setGlobalPlaylistNameEdit(event.currentTarget.value)}
                                  onKeyDown={(event) => { if (event.key === "Enter") void renamePlaylistLocal(); }}
                                  placeholder="Rename"
                                  class="min-w-[120px] border border-[var(--line)] bg-transparent px-2 py-1 font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                                />
                                <button type="button" onClick={() => void renamePlaylistLocal()} class="transition hover:text-[var(--fg)]">Rename</button>
                                <button type="button" onClick={() => void clearVisiblePlaylist()} class="transition hover:text-[var(--fg)]">Clear</button>
                                <button type="button" onClick={() => void deleteVisiblePlaylist()} class="transition hover:text-[var(--fg)]">Delete</button>
                              </div>
                            </div>
                          </section>
                          <div class="flex items-center gap-4 border-b border-[var(--line-soft)] px-4 py-2">
                            <span class="w-8 text-right font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">#</span>
                            <span class="min-w-0 flex-[1.4] font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Track</span>
                            <span class="hidden min-w-0 flex-1 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] md:block">Movie</span>
                            <span class="hidden min-w-0 flex-1 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] lg:block">Music Director</span>
                            <span class="w-20 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Year</span>
                            <span class="w-16 text-right font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]"></span>
                          </div>
                          <Show
                            when={(playlist().tracks || []).length > 0}
                            fallback={
                              <div class="flex min-h-0 flex-1 items-center justify-center px-6 text-center">
                                <div class="text-sm text-[var(--soft)]">Empty playlist. Play a song from Library and add it here.</div>
                              </div>
                            }
                          >
                            <ul ref={listRef} class="min-h-0 flex-1 overflow-y-auto px-2 py-2">
                              <For each={playlist().tracks || []}>
                                {(track, index) => {
                                  const active = () => selectedSong()?.id === track.id;
                                  return (
                                  <li>
                                    <button
                                      ref={(el) => { if (el) { rowRefs.set(track.id, el); } else { rowRefs.delete(track.id); } }}
                                      type="button"
                                      onClick={() => loadSong(track, true)}
                                      class={`flex w-full items-center gap-4 px-4 py-3 text-left transition ${
                                        active()
                                          ? currentTrackId() === track.id
                                            ? "song-row-active text-[var(--fg)]"
                                            : "bg-[var(--hover)] text-[var(--fg)]"
                                          : "bg-transparent text-[var(--fg)] hover:bg-[var(--hover)]"
                                      }`}
                                    >
                                      <span class="w-8 text-right font-mono text-xs text-[var(--soft)]">
                                        {currentTrackId() === track.id && isPlaying() && streamStarted() ? <PlayingBars /> : String(index() + 1).padStart(2, "0")}
                                      </span>
                                      <span class="min-w-0 flex-[1.4] truncate text-sm">{track.track}</span>
                                      <span class="hidden min-w-0 flex-1 truncate font-mono text-[11px] text-[var(--soft)] md:block">{track.movie || "-"}</span>
                                      <span class="hidden min-w-0 flex-1 truncate font-mono text-[11px] text-[var(--soft)] lg:block">{track.musicDirector || "-"}</span>
                                      <span class="w-20 font-mono text-[11px] text-[var(--soft)]">{track.year || "-"}</span>
                                      <span class="flex w-16 justify-end">
                                        <span
                                          role="button"
                                          tabindex="-1"
                                          onClick={(event) => { event.stopPropagation(); void removeSongFromPlaylist(playlist().id, track.id); }}
                                          class="font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--muted)] transition hover:text-[var(--fg)]"
                                        >
                                          Remove
                                        </span>
                                      </span>
                                    </button>
                                  </li>
                                  );
                                }}
                              </For>
                            </ul>
                          </Show>
                        </>
                      )}
                    </Show>
                    <Show when={!playlistDetailLoading() && !myPlaylistDetail()}>
                      <div class="flex min-h-0 flex-1 items-center justify-center px-6 text-center">
                        <div class="text-sm text-[var(--soft)]">
                          {playlists().length > 0 ? "Select a playlist to view its songs." : "Create a playlist to get started."}
                        </div>
                      </div>
                    </Show>
                  </div>
                </div>
              </section>
              </>
            );
          })()}
        </Show>
        <Show when={mainTab() === "library"}>
          <section class="min-h-0 flex-1 overflow-hidden px-6 py-4">
            <div class="grid h-full min-h-0 gap-4 xl:grid-cols-[320px_minmax(0,1fr)]">
              <aside class="min-h-0 overflow-y-auto border border-[var(--line)] bg-[var(--panel)] p-4">
                <div class="flex items-center justify-between gap-3">
                  <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Playlists</div>
                  <Show when={authEnabled() && !user()}>
                    <button
                      type="button"
                      onClick={() => setShowAuthPrompt(true)}
                      aria-label="Create playlist"
                      title="Create playlist"
                      class="flex h-7 w-7 items-center justify-center border border-[var(--line)] bg-[var(--hover)] font-mono text-base leading-none text-[var(--fg)] transition hover:border-[var(--fg)]"
                    >
                      +
                    </button>
                  </Show>
                </div>

                <Show when={libraryProfileEnabled()}>
                  <div class="mt-4 flex items-center gap-2">
                    <input
                      value={playlistNameInput()}
                      onInput={(event) => setPlaylistNameInput(event.currentTarget.value)}
                      placeholder="New playlist"
                      class="min-w-0 flex-1 border border-[var(--line)] bg-transparent px-3 py-2 font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                    />
                    <button
                      type="button"
                      onClick={() => void createPlaylist()}
                      class="border border-[var(--line)] px-3 py-2 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)]"
                    >
                      Create
                    </button>
                  </div>
                </Show>

                <div class="mt-5 space-y-5">
                  <div>
                    <input
                      value={playlistSearchQuery()}
                      onInput={(event) => setPlaylistSearchQuery(event.currentTarget.value)}
                      placeholder="Search playlists"
                      class="w-full border border-[var(--line)] bg-transparent px-3 py-2 font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                    />
                  </div>

                  <Show when={filteredUserPlaylists().length > 0}>
                    <div>
                      <div class="mb-3 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Yours</div>
                      <div class="grid grid-cols-2 gap-2">
                        <For each={filteredUserPlaylists()}>
                          {(playlist) => (
                            <button
                              type="button"
                              onClick={() => void openGlobalPlaylist(playlist.id)}
                              class={`border p-3 text-left transition ${
                                selectedGlobalPlaylistTarget() === playlist.id
                                  ? "border-[var(--fg)] bg-[var(--hover)]"
                                  : "border-[var(--line)] hover:border-[var(--fg)]"
                              }`}
                            >
                              <div class="line-clamp-2 text-sm">{playlist.name}</div>
                              <div class="mt-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]">{playlist.trackCount} tracks</div>
                            </button>
                          )}
                        </For>
                      </div>
                    </div>
                  </Show>

                  <Show when={filteredGlobalPlaylists().length > 0}>
                    <div>
                      <div class="mb-3 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Global</div>
                      <div class="grid grid-cols-2 gap-2">
                        <For each={filteredGlobalPlaylists()}>
                          {(playlist) => (
                            <button
                              type="button"
                              onClick={() => void openGlobalPlaylist(playlist.id)}
                              class={`border p-3 text-left transition ${
                                selectedGlobalPlaylistTarget() === playlist.id
                                  ? "border-[var(--fg)] bg-[var(--hover)]"
                                  : "border-[var(--line)] hover:border-[var(--fg)]"
                              }`}
                            >
                              <div class="line-clamp-2 text-sm">{playlist.name}</div>
                              <div class="mt-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]">{playlist.trackCount} tracks</div>
                            </button>
                          )}
                        </For>
                      </div>
                    </div>
                  </Show>

                  <Show when={normalizedPlaylistSearch() && filteredUserPlaylists().length === 0 && filteredGlobalPlaylists().length === 0}>
                    <div class="border border-[var(--line)] px-3 py-4 text-sm text-[var(--soft)]">
                      No playlists match that search.
                    </div>
                  </Show>
                </div>
              </aside>

              <div class="flex min-h-0 flex-col overflow-hidden border border-[var(--line)] bg-[var(--panel)]">
                <Show when={playlistDetailLoading() && !showPlaylistDetail()}>
                  <div class="flex min-h-0 flex-1 items-center justify-center px-6 text-center">
                    <div>
                      <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Playlist</div>
                      <div class="mt-3 text-sm text-[var(--soft)]">Loading{loadingDots()}</div>
                    </div>
                  </div>
                </Show>
                <Show when={!playlistDetailLoading() && playlistDetailError() && !showPlaylistDetail()}>
                  <div class="flex min-h-0 flex-1 items-center justify-center px-6 text-center">
                    <div>
                      <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Playlist</div>
                      <div class="mt-3 text-sm text-[var(--soft)]">{playlistDetailError()}</div>
                    </div>
                  </div>
                </Show>
                <Show when={visiblePlaylistDetail()}>
                  {(playlist) => (
                    <>
                      <section class="border-b border-[var(--line-soft)] px-4 py-4">
                        <div class="flex items-center justify-between gap-4">
                          <div class="min-w-0">
                            <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Playlist</div>
                            <div class="mt-2 text-lg font-semibold">{playlist().name}</div>
                            <div class="mt-2 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)]">
                              {playlist().source || "manual"} · {(playlist().tracks || []).length} tracks
                            </div>
                          </div>
                          <Show when={canManageVisiblePlaylist()}>
                            <div class="flex flex-wrap items-center gap-3 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)]">
                              <button
                                type="button"
                                onClick={() => void clearVisiblePlaylist()}
                                class="transition hover:text-[var(--fg)]"
                              >
                                Clear
                              </button>
                              <button
                                type="button"
                                onClick={() => void deleteVisiblePlaylist()}
                                class="transition hover:text-[var(--fg)]"
                              >
                                Delete
                              </button>
                            </div>
                          </Show>
                        </div>
                      </section>
                      <div class="flex items-center gap-4 border-b border-[var(--line-soft)] px-4 py-2">
                        <span class="w-8 text-right font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">#</span>
                        <span class="min-w-0 flex-[1.4] font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Song Name</span>
                        <span class="hidden min-w-0 flex-1 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] md:block">Movie</span>
                        <span class="hidden min-w-0 flex-1 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] lg:block">Music Director</span>
                        <span class="w-20 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Year</span>
                        <Show when={canManageVisiblePlaylist()}>
                          <span class="w-16 text-right font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Remove</span>
                        </Show>
                      </div>
                      <Show
                        when={(playlist().tracks || []).length > 0}
                        fallback={
                          <div class="flex min-h-0 flex-1 items-center justify-center px-6 text-center">
                            <div class="text-sm text-[var(--soft)]">No songs in this playlist yet.</div>
                          </div>
                        }
                      >
                        <ul ref={listRef} class="min-h-0 flex-1 overflow-y-auto px-2 py-2">
                          <For each={playlist().tracks || []}>
                            {(track, index) => {
                              const active = () => selectedSong()?.id === track.id;
                              return (
                              <li>
                                <button
                                  ref={(el) => {
                                    if (el) {
                                      rowRefs.set(track.id, el);
                                    } else {
                                      rowRefs.delete(track.id);
                                    }
                                  }}
                                  type="button"
                                  onClick={() => loadSong(track, true)}
                                  class={`flex w-full items-center gap-4 px-4 py-3 text-left transition ${
                                    active()
                                      ? currentTrackId() === track.id
                                        ? "song-row-active text-[var(--fg)]"
                                        : "bg-[var(--hover)] text-[var(--fg)]"
                                      : "bg-transparent text-[var(--fg)] hover:bg-[var(--hover)]"
                                  }`}
                                >
                                  <span class="w-8 text-right font-mono text-xs text-[var(--soft)]">
                                    {currentTrackId() === track.id && isPlaying() && streamStarted() ? <PlayingBars /> : String(index() + 1).padStart(2, "0")}
                                  </span>
                                  <span class="min-w-0 flex-[1.4] truncate text-sm">{track.track}</span>
                                  <span class="hidden min-w-0 flex-1 truncate font-mono text-[11px] text-[var(--soft)] md:block">{track.movie || "-"}</span>
                                  <span class="hidden min-w-0 flex-1 truncate font-mono text-[11px] text-[var(--soft)] lg:block">{track.musicDirector || "-"}</span>
                                  <span class="w-20 font-mono text-[11px] text-[var(--soft)]">{track.year || "-"}</span>
                                  <Show when={canManageVisiblePlaylist()}>
                                    <span class="flex w-16 justify-end">
                                      <span
                                        role="button"
                                        tabindex="-1"
                                        onClick={(event) => {
                                          event.stopPropagation();
                                          void removeSongFromPlaylist(playlist().id, track.id);
                                        }}
                                        class="font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--muted)] transition hover:text-[var(--fg)]"
                                      >
                                        Remove
                                      </span>
                                    </span>
                                  </Show>
                                </button>
                              </li>
                              );
                            }}
                          </For>
                        </ul>
                      </Show>
                    </>
                  )}
                </Show>
                <Show when={!playlistDetailLoading() && (!showPlaylistDetail() || query().trim() || movieFilter() || artistFilter()) && !playlistDetailError()}>
                <section class="border-b border-[var(--line-soft)] px-4 py-3">
                  <div class="flex items-center gap-4 font-mono text-[10px] uppercase tracking-[0.22em]">
                    <button type="button" onClick={() => setSearchTab("songs")} class={searchTab() === "songs" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>Songs</button>
                    <button type="button" onClick={() => setSearchTab("albums")} class={searchTab() === "albums" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>Albums</button>
                    <button type="button" onClick={() => setSearchTab("artists")} class={searchTab() === "artists" ? "text-[var(--fg)]" : "text-[var(--soft)]"}>Artists</button>
                  </div>
                </section>

                <Show when={query().trim() || movieFilter() || artistFilter()}>
                  <section class="border-b border-[var(--line-soft)] px-4 py-4">
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

                <Show when={searchTab() === "songs" || movieFilter() || artistFilter()}>
                  <>
                    <div class="flex items-center gap-4 border-b border-[var(--line-soft)] px-4 py-2">
                      <span class="w-8 text-right font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">#</span>
                      <span class="min-w-0 flex-[1.4] font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Song Name</span>
                      <span class="hidden min-w-0 flex-1 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] md:block">Movie</span>
                      <span class="hidden min-w-0 flex-1 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] lg:block">Music Director</span>
                      <span class="w-20 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Year</span>
                      <Show when={user()}>
                        <span class="w-8 text-right font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Fav</span>
                      </Show>
                    </div>
                    <Show when={!loading()} fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--muted)]">Loading{loadingDots()}</div>}>
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
                                      class={`flex w-full items-center gap-4 px-4 py-3 text-left transition ${
                                        active()
                                          ? currentTrackId() === song.id
                                            ? "song-row-active text-[var(--fg)]"
                                            : "bg-[var(--hover)] text-[var(--fg)]"
                                          : "bg-transparent text-[var(--fg)] hover:bg-[var(--hover)]"
                                      }`}
                                    >
                                      <span class="w-8 text-right font-mono text-xs text-[var(--soft)]">
                                        {currentTrackId() === song.id && isPlaying() && streamStarted() ? <PlayingBars /> : String(index() + 1).padStart(2, "0")}
                                      </span>
                                      <span class="min-w-0 flex-[1.4] truncate text-sm">{song.track}</span>
                                      <span class="hidden min-w-0 flex-1 truncate font-mono text-[11px] text-[var(--soft)] md:block">{song.movie || "-"}</span>
                                      <span class="hidden min-w-0 flex-1 truncate font-mono text-[11px] text-[var(--soft)] lg:block">{song.musicDirector || "-"}</span>
                                      <span class="w-20 font-mono text-[11px] text-[var(--soft)]">{song.year || "-"}</span>
                                      <Show when={user()}>
                                        <span class="flex w-8 justify-end">
                                          <span
                                            role="button"
                                            tabindex="-1"
                                            onClick={(event) => {
                                              event.stopPropagation();
                                              void toggleFavorite(song.id);
                                            }}
                                            class={`transition-colors ${favoriteIdSet().has(song.id) ? "text-[var(--fg)]" : "text-[var(--muted)] hover:text-[var(--fg)]"}`}
                                          >
                                            <HeartIcon filled={favoriteIdSet().has(song.id)} />
                                          </span>
                                        </span>
                                      </Show>
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

                <Show when={searchTab() === "albums" && !movieFilter() && !artistFilter()}>
                  <div class="flex flex-1 items-start overflow-y-auto p-4">
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

                <Show when={searchTab() === "artists" && !movieFilter() && !artistFilter()}>
                  <div class="flex flex-1 items-start overflow-y-auto p-4">
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
                </Show>
              </div>
            </div>
          </section>
        </Show>

        <Show when={mainTab() !== "library" && (mainTab() !== "radio" && mainTab() !== "admin")}>
          <>
            <div class="flex items-center gap-4 border-b border-[var(--line-soft)] px-6 py-2">
              <span class="w-8 text-right font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">#</span>
              <span class="min-w-0 flex-[1.2] font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Song</span>
              <span class="hidden min-w-0 flex-1 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] md:block">Singers</span>
              <span class="hidden min-w-0 flex-1 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] lg:block">Movie</span>
              <span class="hidden min-w-0 flex-1 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] xl:block">Music Director</span>
              <span class="w-20 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Year</span>
              <Show when={user()}>
                <span class="w-8 text-right font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Fav</span>
              </Show>
            </div>

            <Show when={!loading()} fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--muted)]">Loading{loadingDots()}</div>}>
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
                              class={`flex w-full items-center gap-4 px-6 py-3 text-left transition ${
                                active()
                                  ? currentTrackId() === song.id
                                    ? "song-row-active text-[var(--fg)]"
                                    : "bg-[var(--hover)] text-[var(--fg)]"
                                  : "bg-transparent text-[var(--fg)] hover:bg-[var(--hover)]"
                              }`}
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
                              <Show when={user()}>
                                <span class="flex w-8 justify-end">
                                  <span
                                    role="button"
                                    tabindex="-1"
                                    onClick={(event) => {
                                      event.stopPropagation();
                                      void toggleFavorite(song.id);
                                    }}
                                    class={`transition-colors ${favoriteIdSet().has(song.id) ? "text-[var(--fg)]" : "text-[var(--muted)] hover:text-[var(--fg)]"}`}
                                  >
                                    <HeartIcon filled={favoriteIdSet().has(song.id)} />
                                  </span>
                                </span>
                              </Show>
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
              if (radioPlaybackLocked()) {
                return;
              }
              const next = Number(event.currentTarget.value);
              const activeAudio = getActiveAudio();
              if (activeAudio) {
                activeAudio.currentTime = next;
              }
              setCurrentTime(next);
            }}
            class="flex-1"
            disabled={radioPlaybackLocked()}
            style={{ "accent-color": "var(--fg)" }}
          />
          <span class="w-10 font-mono text-[10px] text-[var(--muted)]">{formatTime(duration())}</span>
        </div>

        <div class="grid grid-cols-[1fr_auto_1fr] items-center gap-4">
          <div class="flex items-center gap-2">
            <span class="group relative inline-flex">
              <button
                type="button"
                onClick={() => setShowShortcutHelp(true)}
                aria-label="Show keyboard shortcuts"
                  class="flex h-8 w-8 items-center justify-center rounded-full border border-[var(--line)] text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)]"
              >
                <HelpIcon />
              </button>
              <TooltipBubble text="Keyboard shortcuts" position="bottom-full left-0 mb-2" />
            </span>
            <Show when={user() && currentSong()}>
              <span class="group relative inline-flex">
                <button
                  type="button"
                  onClick={() => void toggleFavorite(currentSong().id)}
                  aria-label={favoriteIdSet().has(currentSong()?.id) ? "Remove from favorites" : "Add to favorites"}
                  class={`flex h-8 w-8 items-center justify-center rounded-full border transition ${
                    favoriteIdSet().has(currentSong()?.id)
                      ? "border-[var(--fg)] text-[var(--fg)]"
                      : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"
                  }`}
                >
                  <HeartIcon filled={favoriteIdSet().has(currentSong()?.id)} />
                </button>
                <TooltipBubble text={favoriteIdSet().has(currentSong()?.id) ? "Remove from favorites" : "Add to favorites"} position="bottom-full left-0 mb-2" />
              </span>
              <span class="group relative inline-flex">
                <button
                  type="button"
                  onClick={() => void saveCurrentToPlaylist()}
                  aria-label="Add to playlist"
                  class="flex h-8 items-center gap-2 rounded-full border border-[var(--line)] px-3 text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)]"
                >
                  <PlusIcon />
                  <span class="font-mono text-[10px] uppercase tracking-[0.18em]">Playlist</span>
                </button>
                <TooltipBubble text="Add to playlist" position="bottom-full left-0 mb-2" />
              </span>
            </Show>
          </div>
          <div class="flex items-center justify-center gap-5">
            <IconButton disabled={radioPlaybackLocked()} onClick={() => selectRelative(-1, true, currentTrackId() || selectedId(), { allowCrossfade: true })} label="Previous">
              <PrevIcon />
            </IconButton>
            <button
              type="button"
              onClick={togglePlayback}
              aria-label={isPlaying() ? (streamStarted() ? "Pause" : "Loading") : "Play"}
              class="flex h-9 w-9 items-center justify-center border border-[var(--fg)] text-[var(--fg)] transition hover:bg-[var(--fg)] hover:text-[var(--bg)]"
            >
              {isPlaying() ? (streamStarted() ? <PauseIcon /> : <LoadingSpinnerIcon />) : <PlayIcon />}
            </button>
            <IconButton disabled={radioPlaybackLocked()} onClick={() => selectRelative(1, true, currentTrackId() || selectedId(), { allowCrossfade: true })} label="Next">
              <NextIcon />
            </IconButton>
            <IconButton
              onClick={cyclePlaybackMode}
              active={repeatMode() !== "off" && !radioPlaybackLocked()}
              disabled={radioPlaybackLocked()}
              label={radioPlaybackLocked() ? "Playback mode locked for radio" : `Playback mode: ${playbackModeLabel()}`}
              class="flex h-5 w-5 items-center justify-center"
            >
              {playbackModeIcon()}
            </IconButton>
          </div>
          <div class="flex items-center justify-end gap-2.5">
            <IconButton onClick={cyclePlaybackSpeed} label={playbackSpeedLabel()} class="flex h-8 min-w-[2.5rem] items-center justify-center">
              <SpeedIcon speed={playbackSpeed()} />
            </IconButton>
            <IconButton onClick={() => setMuted((value) => !value)} label={muted() ? "Unmute" : "Mute"} class="flex h-8 w-8 items-center justify-center">
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
                if (el) {
                  el.dataset.songId = "";
                  el.playbackRate = playbackSpeed();
                  el.defaultPlaybackRate = playbackSpeed();
                }
              }}
              preload="auto"
              onPlay={(event) => {
                if (event.currentTarget === getActiveAudio() && event.currentTarget.dataset.songId === currentTrackId()) {
                  setIsPlaying(true);
                  syncTimelineFromAudio(event.currentTarget);
                }
              }}
              onPlaying={(event) => {
                if (event.currentTarget === getActiveAudio() && event.currentTarget.dataset.songId === currentTrackId()) {
                  setIsPlaying(true);
                  setStreamStarted(true);
                  syncTimelineFromAudio(event.currentTarget);
                }
              }}
              onWaiting={(event) => {
                if (event.currentTarget === getActiveAudio() && event.currentTarget.dataset.songId === currentTrackId()) {
                  setStreamStarted(false);
                }
              }}
              onPause={(event) => {
                if (event.currentTarget === getActiveAudio() && event.currentTarget.dataset.songId === currentTrackId()) {
                  setIsPlaying(false);
                }
              }}
              onLoadedMetadata={(event) => {
                const pending = pendingRadioOffset();
                if (mainTab() === "radio" && pending && currentTrackId() === pending.songId) {
                  const total = Number.isFinite(event.currentTarget.duration) && event.currentTarget.duration > 0 ? event.currentTarget.duration : 0;
                  if (total > 1) {
                    event.currentTarget.currentTime = Math.min(Math.max(0, pending.offsetSeconds || 0), Math.max(0, total - 1));
                  }
                  setPendingRadioOffset(null);
                }
                if (event.currentTarget === getActiveAudio() && event.currentTarget.dataset.songId === currentTrackId()) {
                  syncTimelineFromAudio(event.currentTarget);
                }
              }}
              onTimeUpdate={(event) => {
                if (event.currentTarget === getActiveAudio() && event.currentTarget.dataset.songId === currentTrackId()) {
                  syncTimelineFromAudio(event.currentTarget);
                }
              }}
              onEnded={(event) => {
                if (event.currentTarget !== getActiveAudio() || event.currentTarget.dataset.songId !== currentTrackId()) {
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
                  applyRadioStation(selectedRadioStationId() || radioStations()[0]?.id || "", true);
                  return;
                }
                if (repeatMode() === "album" || repeatMode() === "random") {
                  selectRelative(1, true, currentTrackId() || selectedId(), { allowCrossfade: true });
                  return;
                }
                const current = activeSongList().findIndex((song) => song.id === (currentTrackId() || selectedId()));
                if (current >= 0 && current < activeSongList().length - 1) {
                  selectRelative(1, true, currentTrackId() || selectedId(), { allowCrossfade: true });
                } else {
                  setIsPlaying(false);
                }
              }}
            />
          )}
        </For>
      </footer>
      <Show when={showCreatePlaylistModal()}>
        <div class="absolute inset-0 z-50 bg-black/40">
          <button
            type="button"
            aria-label="Close playlist modal"
            class="absolute inset-0 h-full w-full cursor-default"
            onClick={() => setShowCreatePlaylistModal(false)}
          />
          <div class="absolute left-1/2 top-1/2 w-[min(92vw,420px)] -translate-x-1/2 -translate-y-1/2 border border-[var(--line)] bg-[var(--bg)] p-5 shadow-2xl">
            <div class="flex items-start justify-between gap-4">
              <div>
                <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">
                  {playlists().length ? "Save to playlist" : "Create playlist"}
                </div>
                <div class="mt-2 text-sm text-[var(--soft)]">
                  {playlists().length
                    ? "Choose an existing playlist or create a new one for the current song."
                    : "Create one first, then the current song will be saved into it."}
                </div>
              </div>
              <button
                type="button"
                onClick={() => {
                  setPendingPlaylistSongId("");
                  setShowCreatePlaylistModal(false);
                }}
                class="font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:text-[var(--fg)]"
              >
                Close
              </button>
            </div>
            <div class="mt-5 space-y-4">
              <Show when={playlists().length > 1}>
                <div class="space-y-3 border border-[var(--line)] p-3">
                  <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Existing playlists</div>
                  <select
                    value={selectedPlaylistTarget()}
                    onChange={(event) => setSelectedPlaylistTarget(event.currentTarget.value)}
                    class="w-full bg-transparent font-mono text-sm text-[var(--fg)] outline-none"
                  >
                    <For each={playlists()}>
                      {(playlist) => <option value={playlist.id}>{playlist.name} ({playlist.trackCount})</option>}
                    </For>
                  </select>
                  <div class="flex justify-end">
                    <button
                      type="button"
                      onClick={() => {
                        const song = songIndex().get(pendingPlaylistSongId());
                        if (song && selectedPlaylistTarget()) {
                          void addSongToPlaylistById(selectedPlaylistTarget(), song).then((saved) => {
                            if (saved) {
                              setPendingPlaylistSongId("");
                              setShowCreatePlaylistModal(false);
                            }
                          });
                        }
                      }}
                      class="border border-[var(--fg)] px-4 py-2 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--fg)] transition hover:bg-[var(--fg)] hover:text-[var(--bg)]"
                    >
                      Save to selected
                    </button>
                  </div>
                </div>
              </Show>
              <input
                ref={(el) => {
                  createPlaylistInputRef = el;
                }}
                value={playlistNameInput()}
                onInput={(event) => setPlaylistNameInput(event.currentTarget.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    event.preventDefault();
                    void createPlaylist();
                  }
                }}
                placeholder="Playlist name"
                class="w-full border border-[var(--line)] bg-transparent px-3 py-3 font-mono text-sm text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
              />
              <div class="flex items-center justify-end gap-3">
                <button
                  type="button"
                  onClick={() => {
                    setPendingPlaylistSongId("");
                    setShowCreatePlaylistModal(false);
                  }}
                  class="font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={() => void createPlaylist()}
                  class="border border-[var(--fg)] px-4 py-2 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--fg)] transition hover:bg-[var(--fg)] hover:text-[var(--bg)]"
                >
                  {pendingPlaylistSongId() ? "Create and save" : "Create"}
                </button>
              </div>
            </div>
          </div>
        </div>
      </Show>
      <Show when={authEnabled() && showAuthPrompt()}>
        <div class="absolute inset-0 z-50 bg-black/40">
          <button
            type="button"
            aria-label="Close account prompt"
            class="absolute inset-0 h-full w-full cursor-default"
            onClick={() => setShowAuthPrompt(false)}
          />
          <div class="absolute left-1/2 top-1/2 w-[min(92vw,420px)] -translate-x-1/2 -translate-y-1/2 border border-[var(--line)] bg-[var(--bg)] p-5 shadow-2xl">
            <div class="flex items-start justify-between gap-4">
              <div>
                <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Create account</div>
                <div class="mt-2 text-sm text-[var(--soft)]">{accountMessage() || "Sign in to save favorites and create playlists."}</div>
              </div>
              <button
                type="button"
                onClick={() => setShowAuthPrompt(false)}
                class="font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:text-[var(--fg)]"
              >
                Close
              </button>
            </div>
            <div class="mt-5 flex items-center justify-end gap-3">
              <button
                type="button"
                onClick={() => setShowAuthPrompt(false)}
                class="font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:text-[var(--fg)]"
              >
                Later
              </button>
              <button
                type="button"
                onClick={() => {
                  setShowAuthPrompt(false);
                  beginGoogleLogin();
                }}
                class="border border-[var(--fg)] px-4 py-2 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--fg)] transition hover:bg-[var(--fg)] hover:text-[var(--bg)]"
              >
                Continue with Google
              </button>
            </div>
          </div>
        </div>
      </Show>
    </main>
  );
}

export default App;
