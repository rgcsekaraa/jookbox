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

const PLAYLIST_NAME_MAX_LENGTH = 120;

const sanitizePlaylistName = (value) => String(value || "")
  .replace(/[\x00-\x1f\x7f]+/g, " ")
  .replace(/\s+/g, " ")
  .trim()
  .slice(0, PLAYLIST_NAME_MAX_LENGTH);

const sanitizeSpotifyPlaylistUrl = (value) => String(value || "").replace(/\s+/g, "").trim();

const normalizeText = (value) => String(value || "").trim().toLowerCase();

const SONG_SORT_COLUMNS = {
  default: "#",
  track: "Song Name",
  movie: "Movie",
  musicDirector: "Music Director",
  singers: "Singer",
  year: "Year",
};

const DEFAULT_SONG_SORT = { key: "default", direction: "asc" };

const normalizeSongSortValue = (value) => String(value || "").trim().toLowerCase();
const matchesNormalizedField = (value, needle) => !needle || normalizeText(value).includes(needle);
const albumIdentity = (item) => {
  const albumUrl = String(item?.albumUrl || "").trim();
  if (albumUrl) {
    return albumUrl;
  }
  const albumName = String(item?.album || item?.movie || item?.albumName || "").trim();
  const year = String(item?.year || "").trim();
  return [albumName, year].filter(Boolean).join("::");
};

const compareSongValues = (left, right) => {
  const leftText = String(left || "").trim();
  const rightText = String(right || "").trim();
  const leftBlank = leftText === "";
  const rightBlank = rightText === "";
  if (leftBlank || rightBlank) {
    if (leftBlank && rightBlank) {
      return 0;
    }
    return leftBlank ? 1 : -1;
  }
  const leftNumber = Number(left);
  const rightNumber = Number(right);
  const leftLooksNumeric = Number.isFinite(leftNumber);
  const rightLooksNumeric = Number.isFinite(rightNumber);
  if (leftLooksNumeric && rightLooksNumeric) {
    return leftNumber - rightNumber;
  }
  return normalizeSongSortValue(left).localeCompare(normalizeSongSortValue(right));
};

const sortSongs = (songs, sortConfig) => {
  const list = Array.isArray(songs) ? songs : [];
  const key = sortConfig?.key || "default";
  const direction = sortConfig?.direction === "desc" ? -1 : 1;
  return list
    .map((song, index) => ({ song, index }))
    .sort((left, right) => {
      if (key === "default") {
        return (left.index - right.index) * direction;
      }
      const compared = compareSongValues(left.song?.[key], right.song?.[key]);
      if (compared !== 0) {
        return compared * direction;
      }
      return left.index - right.index;
    })
    .map((entry) => entry.song);
};

const SortableSongHeader = (props) => {
  const isActive = () => props.sortKey === props.columnKey;
  const arrow = () => (isActive() ? (props.sortDirection === "desc" ? "↓" : "↑") : "");
  return (
    <button
      type="button"
      onClick={() => props.onSort?.(props.columnKey)}
      class={`min-w-0 text-left font-mono text-[10px] uppercase tracking-[0.25em] transition ${
        isActive() ? "text-[var(--fg)]" : "text-[var(--faint)] hover:text-[var(--fg)]"
      } ${props.class || ""}`}
    >
      <span>{props.label}</span>
      <Show when={arrow()}>
        <span class="ml-1 inline-block">{arrow()}</span>
      </Show>
    </button>
  );
};

const DrilldownText = (props) => {
  const value = () => String(props.value || "").trim();
  return (
    <span
      role={value() ? "button" : undefined}
      tabindex={value() ? "-1" : undefined}
      onClick={(event) => {
        if (!value()) {
          return;
        }
        event.stopPropagation();
        props.onClick?.(props.payload ?? value());
      }}
      class={value()
        ? `cursor-pointer truncate underline decoration-transparent underline-offset-4 transition hover:text-[var(--fg)] hover:decoration-current ${props.class || ""}`
        : props.class || ""}
      title={value() || undefined}
    >
      {value() || "-"}
    </span>
  );
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

const QueueIcon = () => (
  <svg viewBox="0 0 24 24" class="h-4 w-4 fill-none stroke-current stroke-2">
    <path d="M4 7h10" stroke-linecap="round" />
    <path d="M4 12h8" stroke-linecap="round" />
    <path d="M4 17h6" stroke-linecap="round" />
    <path d="M16 11v6" stroke-linecap="round" />
    <path d="M13 14h6" stroke-linecap="round" />
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

const ChevronUpIcon = () => (
  <svg viewBox="0 0 24 24" class="h-4 w-4 fill-none stroke-current stroke-2">
    <path d="m6 15 6-6 6 6" stroke-linecap="round" stroke-linejoin="round" />
  </svg>
);

const HamburgerIcon = () => (
  <svg viewBox="0 0 24 24" class="h-4 w-4 fill-none stroke-current stroke-2">
    <path d="M4 7h16M4 12h16M4 17h16" stroke-linecap="round" />
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

const SongRowHeartButton = (props) => (
  <Show when={props.show}>
    <span class={`flex items-center justify-end ${props.class || ""}`}>
      <button
        type="button"
        onClick={(event) => {
          event.stopPropagation();
          props.onClick?.();
        }}
        class={`transition-colors ${props.active ? "text-[var(--fg)]" : "text-[var(--muted)] hover:text-[var(--fg)]"}`}
        aria-label={props.active ? "Remove from favorites" : "Add to favorites"}
      >
        <HeartIcon filled={props.active} />
      </button>
    </span>
  </Show>
);

const SongRowQueueButton = (props) => (
  <span class={`flex items-center justify-end ${props.class || ""}`}>
    <button
      type="button"
      onClick={(event) => {
        event.stopPropagation();
        props.onClick?.();
      }}
      class={`transition-colors ${props.active ? "text-[var(--fg)]" : "text-[var(--muted)] hover:text-[var(--fg)]"}`}
      aria-label={props.active ? "Already in queue" : "Add to queue"}
      title={props.active ? "Already in queue" : "Add to queue"}
    >
      <QueueIcon />
    </button>
  </span>
);

const SongRowActions = (props) => (
  <span class={`song-col-action ml-auto flex items-center justify-end gap-3 ${props.class || ""}`}>
    <SongRowQueueButton
      active={props.queued}
      onClick={props.onQueue}
    />
    <SongRowHeartButton
      show={props.showFavorite}
      active={props.favorite}
      onClick={props.onFavorite}
    />
  </span>
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
  const [showRemainingTime, setShowRemainingTime] = createSignal(false);
  const [volume, setVolume] = createSignal(0.9);
  const [muted, setMuted] = createSignal(false);
  const [playbackSpeed, setPlaybackSpeed] = createSignal(1);
  const [repeatMode, setRepeatMode] = createSignal("off");
  const [movieFilter, setMovieFilter] = createSignal("");
  const [albumFilterMeta, setAlbumFilterMeta] = createSignal(null);
  const [artistFilter, setArtistFilter] = createSignal("");
  const [musicDirectorFilter, setMusicDirectorFilter] = createSignal("");
  const [autoplayNext, setAutoplayNext] = createSignal(true);
  const [themePreference, setThemePreference] = createSignal("system");
  const [systemTheme, setSystemTheme] = createSignal("dark");
  const [mainTab, setMainTab] = createSignal("library");
  const activeTheme = createMemo(() => (themePreference() === "system" ? systemTheme() : themePreference()));
  const [recentIds, setRecentIds] = createSignal([]);
  const [playQueue, setPlayQueue] = createSignal([]);
  const [playbackContext, setPlaybackContext] = createSignal({ source: "library", songIds: [] });
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
  const [favoriteAlbums, setFavoriteAlbums] = createSignal([]);
  const [favoriteMusicDirectors, setFavoriteMusicDirectors] = createSignal([]);
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
  const [favoritesTab, setFavoritesTab] = createSignal("songs");
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
  const [showMobileMenu, setShowMobileMenu] = createSignal(false);
  const [showMobilePlayerPanel, setShowMobilePlayerPanel] = createSignal(false);
  const [showMobilePlaylistPicker, setShowMobilePlaylistPicker] = createSignal(false);
  const [mobilePlaylistSection, setMobilePlaylistSection] = createSignal("global");
  const [playlistBrowseSection, setPlaylistBrowseSection] = createSignal("global");
  const [mobilePlayerDragOffset, setMobilePlayerDragOffset] = createSignal(0);
  const [loadingFrame, setLoadingFrame] = createSignal(0);
  const [pendingRadioOffset, setPendingRadioOffset] = createSignal(null);
  const [pendingPlaylistSongId, setPendingPlaylistSongId] = createSignal("");
  const [pendingScrollSongId, setPendingScrollSongId] = createSignal("");
  const [appOffline, setAppOffline] = createSignal(false);
  const [offlineMessage, setOfflineMessage] = createSignal("");
  const [cacheStatus, setCacheStatus] = createSignal(null);
  const [cacheTrimming, setCacheTrimming] = createSignal(false);
  const [cacheMessage, setCacheMessage] = createSignal("");
  const [showSettings, setShowSettings] = createSignal(false);
  const [dbSyncState, setDbSyncState] = createSignal(null);
  const [dbSyncActionBusy, setDbSyncActionBusy] = createSignal(false);
  const [dbSyncActionMessage, setDbSyncActionMessage] = createSignal("");
  const [configReady, setConfigReady] = createSignal(false);
  const [playlistMutationBusy, setPlaylistMutationBusy] = createSignal("");
  const [playlistCreateBusy, setPlaylistCreateBusy] = createSignal(false);
  const [libraryNavStack, setLibraryNavStack] = createSignal([]);
  const [songSortByScope, setSongSortByScope] = createSignal({
    library: DEFAULT_SONG_SORT,
    library_playlist: DEFAULT_SONG_SORT,
    recents: DEFAULT_SONG_SORT,
    favorites: DEFAULT_SONG_SORT,
    queue: DEFAULT_SONG_SORT,
    playlists: DEFAULT_SONG_SORT,
  });

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
  let crossfadeFrame;
  let themeMediaQuery;
  let syncSystemTheme;
  let crossfadeToken = 0;
  let playbackRequestToken = 0;
  let playlistDetailRequestToken = 0;
  let playlistDetailAbortController = null;
  let activeDeckIndex = 0;
  let fadingAudio = null;
  let mobilePlayerTouchStartY = null;
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
    if (query().trim() || movieFilter() || artistFilter() || musicDirectorFilter()) {
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
    if (sync.status === "idle") {
      return "Library auto-update on";
    }
    return "Library auto-update";
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
  const localDbSyncActionLabel = createMemo(() => {
    if (dbSyncActionBusy()) {
      return "Checking...";
    }
    const status = dbSyncState()?.status;
    if (status === "checking") {
      return "Checking...";
    }
    if (status === "downloading") {
      return "Updating...";
    }
    return "Check update";
  });

  const refreshLibrarySnapshot = async () => {
    const [statsResponse, songsResponse] = await Promise.all([
      fetch("/api/stats", { cache: "no-store" }),
      fetch("/api/library", { cache: "no-store" }),
    ]);
    if (!statsResponse.ok || !songsResponse.ok) {
      throw new Error("Unable to refresh library");
    }
    const statsPayload = await statsResponse.json();
    const songsPayload = await songsResponse.json();
    const nextSongs = songsPayload.songs || [];
    setStats(statsPayload);
    setSongs(nextSongs);
    setResults((current) => ({
      songs: query().trim() ? current.songs || [] : nextSongs.slice(0, 200),
      albums: current.albums || [],
      artists: current.artists || [],
    }));
    if (!nextSongs.some((song) => song.id === selectedId())) {
      setSelectedId(nextSongs[0]?.id || "");
    }
    worker?.postMessage({ type: "index", payload: nextSongs });
    sendSearch(query().trim().toLowerCase());
  };

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
    if (appOffline()) {
      return;
    }
    const localLimit = localMode() ? 3 : 8;
    const filteredIds = [...new Set(ids.filter(Boolean))]
      .filter((id) => !prefetchedIds.has(id))
      .slice(0, localLimit);
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
    }, localMode() ? 220 : 120);
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
      markAppOffline("No internet connection detected.");
      return false;
    }
    if (localMode()) {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 4000);
      try {
        const response = await fetch("/api/health", {
          cache: "no-store",
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error("Local backend is not responding.");
        }
      } catch {
        markAppOffline("Local backend is not responding. Docker may need a restart.");
        return false;
      } finally {
        clearTimeout(timeoutId);
      }
    }
    if (appOffline()) {
      markAppOnline();
    }
    return true;
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

  const checkForLibraryUpdate = async () => {
    if (!localMode() || dbSyncActionBusy()) {
      return;
    }
    setDbSyncActionBusy(true);
    try {
      const previousVersion = dbSyncState()?.currentVersion || "";
      const response = await fetch("/api/db-sync/check", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ force: false }),
      });
      if (!response.ok) {
        throw new Error("Unable to check for library updates");
      }
      const payload = await response.json();
      const sync = payload.sync || null;
      setDbSyncState(sync);
      setDbSyncActionMessage(sync?.message || "Library status updated");
      if (sync?.status === "idle") {
        await refreshLibrarySnapshot();
      }
      if (sync?.currentVersion && sync.currentVersion !== previousVersion) {
        setDbSyncActionMessage("Library updated successfully");
      }
    } catch (syncError) {
      setDbSyncActionMessage(syncError?.message || "Unable to check for library updates");
    } finally {
      setDbSyncActionBusy(false);
    }
  };

  const visibleResults = createMemo(() => {
    const normalizedQuery = normalizeText(query());
    const sourceSongs = movieFilter() || artistFilter() || musicDirectorFilter()
      ? songs()
      : (normalizedQuery ? (results().songs || []) : songs());
    const filteredByAlbum = movieFilter() ? sourceSongs.filter((song) => albumIdentity(song) === movieFilter()) : sourceSongs;
    const filteredByArtist = artistFilter()
      ? filteredByAlbum.filter((song) => normalizeText(song.singers).includes(normalizeText(artistFilter())))
      : filteredByAlbum;
    const filteredByDirector = musicDirectorFilter()
      ? filteredByArtist.filter((song) => normalizeText(song.musicDirector) === normalizeText(musicDirectorFilter()))
      : filteredByArtist;
    if (!normalizedQuery) {
      return filteredByDirector;
    }
    return filteredByDirector.filter((song) => [
      song.track,
      song.movie,
      song.musicDirector,
      song.singers,
      song.year,
    ].some((value) => matchesNormalizedField(value, normalizedQuery)));
  });
  const visibleAlbums = createMemo(() => {
    const normalizedQuery = normalizeText(query());
    const sourceSongs = musicDirectorFilter() || artistFilter() || movieFilter() ? visibleResults() : songs();
    const grouped = new Map();
    for (const song of sourceSongs) {
      const albumName = String(song.movie || "").trim();
      if (!albumName) {
        continue;
      }
      if (
        normalizedQuery &&
        ![
          song.movie,
          song.musicDirector,
          song.year,
          song.track,
          song.singers,
        ].some((value) => matchesNormalizedField(value, normalizedQuery))
      ) {
        continue;
      }
      const existing = grouped.get(albumName);
      if (existing) {
        existing.count += 1;
        if (song.year && (!existing.year || Number(song.year) > Number(existing.year))) {
          existing.year = song.year;
        }
      } else {
        grouped.set(albumName, {
          album: albumName,
          albumUrl: song.albumUrl || "",
          albumKey: albumIdentity(song),
          musicDirector: song.musicDirector,
          year: song.year,
          count: 1,
        });
      }
    }
    return Array.from(grouped.values()).sort((a, b) => {
      if (musicDirectorFilter()) {
        const yearDiff = (Number(b.year) || 0) - (Number(a.year) || 0);
        if (yearDiff !== 0) return yearDiff;
      }
      return (Number(b.year) || 0) - (Number(a.year) || 0) || b.count - a.count || a.album.localeCompare(b.album);
    });
  });
  const visibleArtists = createMemo(() => {
    if (musicDirectorFilter() || movieFilter()) {
      const grouped = new Map();
      for (const song of visibleResults()) {
        for (const singer of (song.singers || "").split(/,|&|\/| feat\. | featuring /i).map((s) => s.trim()).filter(Boolean)) {
          const existing = grouped.get(singer);
          if (existing) {
            existing.count += 1;
          } else {
            grouped.set(singer, { artist: singer, count: 1 });
          }
        }
      }
      return Array.from(grouped.values()).sort((a, b) => b.count - a.count || a.artist.localeCompare(b.artist));
    }
    return results().artists || [];
  });
  const visibleMusicDirectors = createMemo(() => {
    const grouped = new Map();
    const normalizedQuery = normalizeText(query());
    const sourceSongs = movieFilter() || artistFilter() || musicDirectorFilter() ? visibleResults() : songs();
    for (const song of sourceSongs) {
      const director = String(song.musicDirector || "").trim();
      if (!director) {
        continue;
      }
      if (
        normalizedQuery &&
        ![
          director,
          song.movie,
          song.year,
          song.track,
          song.singers,
        ].some((value) => matchesNormalizedField(value, normalizedQuery))
      ) {
        continue;
      }
      const existing = grouped.get(director);
      if (existing) {
        existing.count += 1;
        if (song.year && (!existing.latestYear || Number(song.year) > Number(existing.latestYear))) {
          existing.latestYear = song.year;
        }
      } else {
        grouped.set(director, {
          musicDirector: director,
          count: 1,
          latestYear: song.year || "",
        });
      }
    }
    return Array.from(grouped.values()).sort((a, b) =>
      (Number(b.latestYear) || 0) - (Number(a.latestYear) || 0) ||
      b.count - a.count ||
      a.musicDirector.localeCompare(b.musicDirector)
    );
  });
  const songIndex = createMemo(() => new Map(songs().map((song) => [song.id, song])));
  const recentSongs = createMemo(() => recentIds().map((id) => songIndex().get(id)).filter(Boolean));
  const queueEntrySongId = (entry) => typeof entry === "string" ? entry : entry?.songId;
  const queueEntryId = (entry) => typeof entry === "string" ? entry : entry?.entryId;
  const makeQueueEntry = (songId) => ({
    entryId: `${songId}:${Date.now()}:${Math.random().toString(36).slice(2)}`,
    songId,
  });
  const queueSongIds = createMemo(() => playQueue().map(queueEntrySongId).filter(Boolean));
  const queuedSongs = createMemo(() => playQueue()
    .map((entry, index) => {
      const songId = queueEntrySongId(entry);
      const song = songIndex().get(songId);
      return song ? { ...song, queueEntryId: queueEntryId(entry) || `${songId}:${index}` } : null;
    })
    .filter(Boolean));
  const queuedSongIds = createMemo(() => new Set(queueSongIds()));
  const favoriteSongs = createMemo(() => favoriteIds().map((id) => songIndex().get(id)).filter(Boolean));
  const favoriteAlbumSet = createMemo(() => new Set(favoriteAlbums().map((album) => albumIdentity(album))));
  const favoriteMusicDirectorSet = createMemo(() => new Set(favoriteMusicDirectors()));
  const favoriteAlbumRows = createMemo(() => {
    const grouped = new Map();
    for (const song of songs()) {
      const albumName = String(song.movie || "").trim();
      const albumKey = albumIdentity(song);
      if (!albumName || !favoriteAlbumSet().has(albumKey)) {
        continue;
      }
      const existing = grouped.get(albumKey);
      if (existing) {
        existing.count += 1;
        if (song.year && (!existing.year || Number(song.year) > Number(existing.year))) {
          existing.year = song.year;
        }
      } else {
        grouped.set(albumKey, {
          album: albumName,
          albumUrl: song.albumUrl || "",
          albumKey,
          musicDirector: song.musicDirector || "",
          year: song.year || "",
          count: 1,
        });
      }
    }
    return favoriteAlbums()
      .map((album) => grouped.get(albumIdentity(album)))
      .filter(Boolean);
  });
  const favoriteMusicDirectorRows = createMemo(() => {
    const grouped = new Map();
    for (const song of songs()) {
      const director = String(song.musicDirector || "").trim();
      if (!director || !favoriteMusicDirectorSet().has(director)) {
        continue;
      }
      const existing = grouped.get(director);
      if (existing) {
        existing.count += 1;
        if (song.year && (!existing.latestYear || Number(song.year) > Number(existing.latestYear))) {
          existing.latestYear = song.year;
        }
      } else {
        grouped.set(director, {
          musicDirector: director,
          count: 1,
          latestYear: song.year || "",
        });
      }
    }
    return favoriteMusicDirectors()
      .map((director) => grouped.get(director))
      .filter(Boolean);
  });
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
  const songSortScope = createMemo(() => {
    if (mainTab() === "library" && showPlaylistDetail()) {
      return "library_playlist";
    }
    if (mainTab() === "library") {
      return "library";
    }
    if (mainTab() === "recents") {
      return "recents";
    }
    if (mainTab() === "favorites") {
      return "favorites";
    }
    if (mainTab() === "queue") {
      return "queue";
    }
    if (mainTab() === "playlists") {
      return "playlists";
    }
    return "library";
  });
  const currentSongSort = createMemo(() => songSortByScope()[songSortScope()] || DEFAULT_SONG_SORT);
  const activeSongList = createMemo(() => {
    if (mainTab() === "recents") {
      return recentSongs();
    }
    if (mainTab() === "favorites") {
      if (favoritesTab() !== "songs") {
        return [];
      }
      return favoriteSongs();
    }
    if (mainTab() === "radio") {
      return radioQueue();
    }
    if (mainTab() === "queue") {
      return queuedSongs();
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
  const sortedActiveSongList = createMemo(() => {
    if (mainTab() === "radio" || mainTab() === "queue") {
      return activeSongList();
    }
    return sortSongs(activeSongList(), currentSongSort());
  });
  const selectedSong = createMemo(() => {
    const visible = sortedActiveSongList();
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
      sortedActiveSongList().find((song) => song.id === currentId) ||
      (globalPlaylistDetail()?.tracks || []).find((song) => song.id === currentId) ||
      songs().find((song) => song.id === currentId) ||
      null
    );
  });
  const playbackContextSongs = createMemo(() => playbackContext().songIds.map((id) => songIndex().get(id)).filter(Boolean));
  const selectedActiveSong = createMemo(() => sortedActiveSongList().find((song) => song.id === selectedId()) || null);
  const selectedIndex = createMemo(() => sortedActiveSongList().findIndex((song) => song.id === selectedId()));
  const favoriteIdSet = createMemo(() => new Set(favoriteIds()));
  const loadingDots = createMemo(() => ".".repeat((loadingFrame() % 3) + 1));
  const pushLibraryNavState = () => {
    const playlistDetail = globalPlaylistDetail();
    setLibraryNavStack((current) => [
      ...current,
      {
        mainTab: mainTab(),
        query: query(),
        searchTab: searchTab(),
        movieFilter: movieFilter(),
        albumFilterMeta: albumFilterMeta(),
        artistFilter: artistFilter(),
        musicDirectorFilter: musicDirectorFilter(),
        selectedId: selectedId(),
        selectedGlobalPlaylistTarget: selectedGlobalPlaylistTarget(),
        globalPlaylistDetail: playlistDetail
          ? {
              ...playlistDetail,
              tracks: Array.isArray(playlistDetail.tracks) ? [...playlistDetail.tracks] : [],
            }
          : null,
      },
    ]);
  };

  const navigateToMovie = (movie) => {
    const payload = typeof movie === "object" && movie !== null
      ? movie
      : { album: String(movie || "").trim(), albumUrl: "", year: "" };
    const nextMovie = albumIdentity(payload);
    if (!nextMovie) {
      return;
    }
    if (mainTab() === "library" && movieFilter() === nextMovie && !artistFilter() && !musicDirectorFilter()) {
      return;
    }
    pushLibraryNavState();
    setMainTab("library");
    setQuery("");
    setSearchTab("songs");
    setMovieFilter(nextMovie);
    setAlbumFilterMeta({
      album: String(payload.album || payload.movie || "").trim(),
      albumUrl: String(payload.albumUrl || "").trim(),
      year: String(payload.year || "").trim(),
    });
    setArtistFilter("");
    setMusicDirectorFilter("");
  };

  const navigateToMusicDirector = (musicDirector) => {
    const nextDirector = String(musicDirector || "").trim();
    if (!nextDirector) {
      return;
    }
    if (mainTab() === "library" && musicDirectorFilter() === nextDirector && !movieFilter() && !artistFilter()) {
      return;
    }
    pushLibraryNavState();
    setMainTab("library");
    setQuery("");
    setSearchTab("albums");
    setMovieFilter("");
    setAlbumFilterMeta(null);
    setArtistFilter("");
    setMusicDirectorFilter(nextDirector);
  };

  const restorePreviousLibraryView = () => {
    const history = libraryNavStack();
    const previous = history[history.length - 1];
    if (!previous) {
      return;
    }
    setLibraryNavStack((current) => current.slice(0, -1));
    setMainTab(previous.mainTab || "library");
    setQuery(previous.query || "");
    setSearchTab(previous.searchTab || "songs");
    setMovieFilter(previous.movieFilter || "");
    setAlbumFilterMeta(previous.albumFilterMeta || null);
    setArtistFilter(previous.artistFilter || "");
    setMusicDirectorFilter(previous.musicDirectorFilter || "");
    setSelectedGlobalPlaylistTarget(previous.selectedGlobalPlaylistTarget || "");
    setGlobalPlaylistDetail(previous.globalPlaylistDetail || null);
    setGlobalPlaylistNameEdit(previous.globalPlaylistDetail?.name || "");
    setPlaylistDetailError("");
    setPlaylistDetailLoading(false);
    const previousTracks = previous.globalPlaylistDetail?.tracks || [];
    const currentPlayingId = currentTrackId();
    const targetSongId = previousTracks.some((song) => song.id === currentPlayingId)
      ? currentPlayingId
      : (previous.selectedId || selectedId());
    if (targetSongId) {
      setSelectedId(targetSongId);
      setPendingScrollSongId(targetSongId);
    }
    if (previous.globalPlaylistDetail?.id) {
      setPlaylistDetailCache((current) => {
        const next = new Map(current);
        next.set(previous.globalPlaylistDetail.id, previous.globalPlaylistDetail);
        return next;
      });
    }
  };

  const openCurrentSongAlbum = (song) => {
    if (!song?.movie) {
      return;
    }
    navigateToMovie({ album: song.movie, albumUrl: song.albumUrl, year: song.year });
    setShowMobilePlayerPanel(false);
  };
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
  const toggleSongSort = (columnKey) => {
    if (mainTab() === "radio" || mainTab() === "admin") {
      return;
    }
    setSongSortByScope((current) => {
      const scope = songSortScope();
      const existing = current[scope] || DEFAULT_SONG_SORT;
      const nextSort = existing.key === columnKey
        ? { key: columnKey, direction: existing.direction === "asc" ? "desc" : "asc" }
        : { key: columnKey, direction: "asc" };
      return {
        ...current,
        [scope]: nextSort,
      };
    });
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
    const allowedMainTabs = ["library", "recents", "favorites", "queue"];
    if (localMode()) {
      allowedMainTabs.push("playlists");
    }
    if (radioEnabled()) {
      allowedMainTabs.push("radio", "admin");
    }
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
    if (tab === "playlists") {
      setShowMobilePlaylistPicker(false);
      setPlaylistSearchQuery("");
      setMobilePlaylistSection("global");
    }
    if (tab === "admin" || tab === "playlists" || tab === "queue") {
      if (tab === "queue" && queuedSongs()[0]) {
        setSelectedId(queuedSongs()[0].id);
      }
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
      : tab === "favorites" ? (favoritesTab() === "songs" ? favoriteSongs() : [])
      : tab === "library" ? visibleResults()
      : sortedActiveSongList();
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
    if (!user()?.is_admin || !currentRadioStation() || playlistMutationBusy()) {
      return;
    }
    const requestedName = sanitizePlaylistName(radioSaveName()) || sanitizePlaylistName(currentRadioStation().name);
    const body = {
      mode: radioSaveMode(),
      targetPlaylistId: radioSaveMode() === "overwrite" ? selectedGlobalPlaylistTarget() : "",
      name: requestedName,
    };
    setPlaylistMutationBusy("radio-save");
    try {
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
    } finally {
      setPlaylistMutationBusy("");
    }
  };

  const getAudio = (index) => audioRefs[index] || null;
  const getActiveAudio = () => getAudio(activeDeckIndex);
  const getInactiveAudio = () => getAudio(activeDeckIndex === 0 ? 1 : 0);
  const isSongLoadedInAnyDeck = (songId) => audioRefs.some((audio) => audio?.dataset.songId === songId);
  const nextPlaybackRequestToken = () => {
    playbackRequestToken += 1;
    return playbackRequestToken;
  };
  const isCurrentPlaybackRequest = (token, songId) => token === playbackRequestToken && currentTrackId() === songId;
  const resetAudioDeck = (audio) => {
    if (!audio) {
      return;
    }
    audio.pause();
    audio.currentTime = 0;
    audio.dataset.songId = "";
    audio.removeAttribute("src");
    audio.load();
    audio.volume = 0;
  };

  const stopCrossfade = () => {
    crossfadeToken += 1;
    cancelAnimationFrame(crossfadeFrame);
    if (fadingAudio) {
      resetAudioDeck(fadingAudio);
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
    resetAudioDeck(inactive);
  };

  const forceStopInactiveDecks = (activeAudio) => {
    stopCrossfade();
    audioRefs.forEach((audio) => {
      if (audio && audio !== activeAudio) {
        resetAudioDeck(audio);
      }
    });
  };

  const playPrimaryDeck = (audio, song, relativeUrl, requestToken = playbackRequestToken) => {
    if (!audio || !song || !relativeUrl) {
      return Promise.reject(new Error("Primary deck unavailable"));
    }
    stopCrossfade();
    resetInactiveDeck();
    audio.pause();
    audio.currentTime = 0;
    audio.dataset.songId = song.id;
    audio.src = relativeUrl;
    audio.preload = "auto";
    audio.load();
    syncTimelineFromAudio(audio, true);
    return audio.play().then(() => {
      if (!isCurrentPlaybackRequest(requestToken, song.id)) {
        if (audio.dataset.songId === song.id) {
          resetAudioDeck(audio);
        }
        return;
      }
      promoteDeck(activeDeckIndex);
      audio.volume = muted() ? 0 : volume();
      setIsPlaying(true);
      setStreamStarted(true);
      syncTimelineFromAudio(audio, true);
    });
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
        resetAudioDeck(fromAudio);
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
      const sessionPayload = await sessionResponse.json().catch(() => ({}));
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
          const preferredGlobalPlaylistId = nextGlobalPlaylists.find((playlist) => playlist.source !== "dynamic")?.id || nextGlobalPlaylists[0]?.id || "";
          const defaultPlaylistId = globalPlaylistDetail()?.id || selectedGlobalPlaylistTarget() || nextPlaylists[0]?.id || preferredGlobalPlaylistId || "";
          if (defaultPlaylistId) {
            setSelectedGlobalPlaylistTarget(defaultPlaylistId);
            if (!globalPlaylistDetail()) {
              void openGlobalPlaylist(defaultPlaylistId);
            }
          }
        } else {
          setGlobalPlaylists([]);
        }
        setFavoriteIds([]);
        setFavoriteAlbums([]);
        setFavoriteMusicDirectors([]);
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
        setFavoriteAlbums(favoritesPayload.albums || []);
        setFavoriteMusicDirectors(favoritesPayload.musicDirectors || []);
      } else {
        setFavoriteIds([]);
        setFavoriteAlbums([]);
        setFavoriteMusicDirectors([]);
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
        const preferredGlobalPlaylistId = nextGlobalPlaylists.find((playlist) => playlist.source !== "dynamic")?.id || nextGlobalPlaylists[0]?.id || "";
        const defaultPlaylistId = globalPlaylistDetail()?.id || selectedGlobalPlaylistTarget() || nextPlaylists[0]?.id || preferredGlobalPlaylistId || "";
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
      setFavoriteAlbums([]);
      setFavoriteMusicDirectors([]);
      setAdminUsers([]);
      setAirflowStatus(null);
      if (!localMode()) {
        setPlaylists([]);
        setGlobalPlaylists([]);
        setPlaylistDetailCache(new Map());
        setPlaylistDetailLoading(false);
        setPlaylistDetailError("");
        setSelectedPlaylistTarget("");
        setSelectedGlobalPlaylistTarget("");
        setPreferenceStore("guest");
        resetUserScopedPreferences();
      } else {
        setPreferenceStore("db");
        setPreferencesReady(true);
      }
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
    setFavoriteAlbums([]);
    setFavoriteMusicDirectors([]);
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

  const toggleAlbumFavorite = async (album) => {
    const payload = typeof album === "object" && album !== null
      ? album
      : { albumName: String(album || "").trim(), albumUrl: "" };
    const albumName = String(payload.albumName || payload.album || payload.movie || "").trim();
    const albumUrl = String(payload.albumUrl || "").trim();
    const albumKey = albumIdentity({ albumName, albumUrl, year: payload.year });
    if (!albumKey) {
      return;
    }
    if (!user()) {
      setAccountMessage("Create an account to save favorites");
      setShowAuthPrompt(true);
      return;
    }
    const liked = favoriteAlbumSet().has(albumKey);
    const response = await fetch("/api/favorites/albums", {
      method: liked ? "DELETE" : "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ albumName, albumUrl }),
    });
    if (response.ok) {
      setFavoriteAlbums((current) =>
        liked
          ? current.filter((value) => albumIdentity(value) !== albumKey)
          : [{ albumName, albumUrl, year: String(payload.year || "").trim() }, ...current]
      );
      return;
    }
    setAccountMessage("Unable to update album favorites");
  };

  const toggleMusicDirectorFavorite = async (musicDirector) => {
    const normalizedDirector = String(musicDirector || "").trim();
    if (!normalizedDirector) {
      return;
    }
    if (!user()) {
      setAccountMessage("Create an account to save favorites");
      setShowAuthPrompt(true);
      return;
    }
    const liked = favoriteMusicDirectorSet().has(normalizedDirector);
    const response = await fetch("/api/favorites/music-directors", {
      method: liked ? "DELETE" : "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ musicDirector: normalizedDirector }),
    });
    if (response.ok) {
      setFavoriteMusicDirectors((current) =>
        liked ? current.filter((value) => value !== normalizedDirector) : [normalizedDirector, ...current]
      );
      return;
    }
    setAccountMessage("Unable to update music director favorites");
  };

  const createPlaylist = async () => {
    const name = sanitizePlaylistName(playlistNameInput());
    if (playlistCreateBusy()) {
      return;
    }
    if (!user()) {
      setAccountMessage("Create an account to make playlists");
      setShowAuthPrompt(true);
      return;
    }
    if (!name) {
      return;
    }
    setPlaylistCreateBusy(true);
    try {
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
    } finally {
      setPlaylistCreateBusy(false);
    }
  };

  const createGlobalPlaylist = async () => {
    const name = sanitizePlaylistName(globalPlaylistNameInput());
    if (!name || !user()?.is_admin || playlistMutationBusy()) {
      return;
    }
    setPlaylistMutationBusy("create-global");
    try {
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
    } finally {
      setPlaylistMutationBusy("");
    }
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
    const normalizedUrl = sanitizeSpotifyPlaylistUrl(spotifyImportUrl());
    if (!user() || !normalizedUrl) {
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
      body: JSON.stringify({ url: normalizedUrl, accessToken }),
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
    if (playlistDetailAbortController) {
      playlistDetailAbortController.abort();
    }
    const controller = new AbortController();
    playlistDetailAbortController = controller;
    const requestToken = ++playlistDetailRequestToken;
    setLibraryNavStack([]);
    setQuery("");
    setMovieFilter("");
    setAlbumFilterMeta(null);
    setArtistFilter("");
    setMusicDirectorFilter("");
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
      let response = null;
      let payload = {};
      for (let attempt = 0; attempt < 2; attempt += 1) {
        response = await fetch(`/api/playlists/${playlistId}`, {
          cache: "no-store",
          signal: controller.signal,
        });
        payload = await response.json().catch(() => ({}));
        if (response.ok || controller.signal.aborted || attempt === 1) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 180));
      }
      if (requestToken !== playlistDetailRequestToken) {
        return;
      }
      if (!response.ok) {
        if (!cachedPlaylist) {
          setGlobalPlaylistDetail(null);
        }
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
      if (error?.name === "AbortError") {
        return;
      }
      if (requestToken !== playlistDetailRequestToken) {
        return;
      }
      if (!cachedPlaylist) {
        setGlobalPlaylistDetail(null);
      }
      setPlaylistDetailError(error?.message || "Unable to load playlist");
      setAccountMessage(error?.message || "Unable to load playlist");
    } finally {
      if (playlistDetailAbortController === controller) {
        playlistDetailAbortController = null;
      }
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
    setAlbumFilterMeta(null);
    setArtistFilter("");
    setMusicDirectorFilter("");
    setSearchTab("songs");
  };

  const removeSongFromPlaylist = async (playlistId, songId) => {
    const mutationKey = `remove:${playlistId}:${songId}`;
    if (!canManageVisiblePlaylist() || playlistMutationBusy()) {
      return;
    }
    setPlaylistMutationBusy(mutationKey);
    try {
      const response = await fetch(`/api/playlists/${playlistId}/songs/${songId}`, { method: "DELETE" });
      if (!response.ok) {
        setAccountMessage("Unable to remove song from playlist");
        return;
      }
      await refreshAccountState();
      await openGlobalPlaylist(playlistId);
    } finally {
      setPlaylistMutationBusy("");
    }
  };

  const clearVisiblePlaylist = async () => {
    const playlist = globalPlaylistDetail();
    if (!playlist || !canManageVisiblePlaylist() || playlistMutationBusy()) {
      return;
    }
    setPlaylistMutationBusy(`clear:${playlist.id}`);
    try {
      const response = await fetch(`/api/playlists/${playlist.id}/songs`, { method: "DELETE" });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        setAccountMessage(payload.message || "Unable to clear playlist");
        return;
      }
      await refreshAccountState();
      await openGlobalPlaylist(playlist.id);
      setAccountMessage("Playlist cleared");
    } finally {
      setPlaylistMutationBusy("");
    }
  };

  const deleteVisiblePlaylist = async () => {
    const playlist = globalPlaylistDetail();
    if (!playlist || !canManageVisiblePlaylist() || playlistMutationBusy()) {
      return;
    }
    setPlaylistMutationBusy(`delete:${playlist.id}`);
    try {
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
    } finally {
      setPlaylistMutationBusy("");
    }
  };

  const renamePlaylistLocal = async () => {
    const playlist = globalPlaylistDetail();
    if (!user() || !playlist || playlistMutationBusy()) {
      return;
    }
    const name = sanitizePlaylistName(globalPlaylistNameEdit());
    setPlaylistMutationBusy(`rename:${playlist.id}`);
    try {
      const response = await fetch(`/api/playlists/${playlist.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      const payload = await response.json();
      if (!response.ok) {
        setAccountMessage(payload.message || "Unable to rename playlist");
        return;
      }
      await refreshAccountState();
      await openGlobalPlaylist(playlist.id);
      setAccountMessage("Playlist renamed");
    } finally {
      setPlaylistMutationBusy("");
    }
  };

  const renameGlobalPlaylist = async () => {
    const playlist = globalPlaylistDetail();
    if (!user()?.is_admin || !playlist || playlistMutationBusy()) {
      return;
    }
    const name = sanitizePlaylistName(globalPlaylistNameEdit());
    setPlaylistMutationBusy(`rename:${playlist.id}`);
    try {
      const response = await fetch(`/api/playlists/${playlist.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      const payload = await response.json();
      if (!response.ok) {
        setAdminMessage(payload.message || "Unable to rename playlist");
        return;
      }
      await refreshAccountState();
      await openGlobalPlaylist(playlist.id);
      setAdminMessage("Playlist renamed");
    } finally {
      setPlaylistMutationBusy("");
    }
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
    const list = sortedActiveSongList();
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

  const setPlaybackContextForSong = (song, options = {}) => {
    if (!song?.id || options.preservePlaybackContext) {
      return;
    }
    const providedIds = Array.isArray(options.playbackContextIds) ? options.playbackContextIds : [];
    const visibleIds = sortedActiveSongList().map((item) => item.id).filter(Boolean);
    const sourceIds = providedIds.length ? providedIds : visibleIds;
    const nextIds = [];
    for (const id of sourceIds) {
      if (id && !nextIds.includes(id)) {
        nextIds.push(id);
      }
    }
    if (!nextIds.includes(song.id)) {
      nextIds.unshift(song.id);
    }
    setPlaybackContext({
      source: options.playbackContextSource || mainTab(),
      songIds: nextIds,
    });
  };

  const loadSong = (song, autoplay = false, options = {}) => {
    const { allowCrossfade = false, forceImmediate = false } = options;
    const activeAudio = getActiveAudio();
    const inactiveAudio = getInactiveAudio();
    if (!song || !activeAudio || !inactiveAudio) {
      return;
    }
    const requestToken = nextPlaybackRequestToken();

    const previousTrackId = currentTrackId();
    const isSameSong = previousTrackId === song.id;
    setSelectedId(song.id);
    setCurrentTrackId(song.id);
    setPlaybackContextForSong(song, options);
    rememberRecentSong(song.id);
    prefetchSongIds(getSongNeighborhoodIds(song));

    const version = encodeURIComponent(song.updatedAt || song.id);
    const nextRelativeUrl = `${song.audioUrl}?v=${version}`;
    const nextUrl = new URL(nextRelativeUrl, window.location.origin).href;
    const canCrossfade = Boolean(
      !forceImmediate &&
      allowCrossfade &&
      autoplay &&
      activeAudio.src &&
      !activeAudio.paused &&
      previousTrackId &&
      previousTrackId !== song.id
    );
    if (activeAudio.src !== nextUrl) {
      if (forceImmediate) {
        forceStopInactiveDecks(activeAudio);
      } else {
        stopCrossfade();
      }
      setCurrentTime(0);
      setDuration(0);
      setStreamStarted(false);
      if (!canCrossfade) {
        activeAudio.dataset.songId = song.id;
      }
      if (!canCrossfade) {
        if (autoplay) {
          void playPrimaryDeck(activeAudio, song, nextRelativeUrl, requestToken).catch(() => {
            if (isCurrentPlaybackRequest(requestToken, song.id)) {
              setIsPlaying(false);
              setStreamStarted(false);
            }
          });
          return;
        }
        activeAudio.pause();
        activeAudio.currentTime = 0;
        activeAudio.src = nextRelativeUrl;
        activeAudio.dataset.songId = song.id;
        activeAudio.preload = "auto";
        activeAudio.load();
        resetInactiveDeck();
        syncTimelineFromAudio(activeAudio, true);
        return;
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
            if (!isCurrentPlaybackRequest(requestToken, song.id)) {
              if (inactiveAudio.dataset.songId === song.id) {
                resetAudioDeck(inactiveAudio);
              }
              return;
            }
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
            if (!isCurrentPlaybackRequest(requestToken, song.id)) {
              if (inactiveAudio.dataset.songId === song.id) {
                resetAudioDeck(inactiveAudio);
              }
              return;
            }
            if (canCrossfade) {
              if (isCurrentPlaybackRequest(requestToken, song.id)) {
                void playPrimaryDeck(activeAudio, song, nextRelativeUrl, requestToken)
                  .catch(() => {
                    if (isCurrentPlaybackRequest(requestToken, song.id)) {
                      setIsPlaying(false);
                      setStreamStarted(false);
                    }
                  });
              }
              return;
            }
            void playPrimaryDeck(activeAudio, song, nextRelativeUrl, requestToken)
              .catch(() => {
                if (isCurrentPlaybackRequest(requestToken, song.id)) {
                  setIsPlaying(false);
                  setStreamStarted(false);
                }
              });
          });
      } else if (!isSameSong) {
        promoteDeck(activeDeckIndex === 0 ? 1 : 0);
        activeAudio.pause();
      }
      return;
    }

    activeAudio.dataset.songId = song.id;
    if (autoplay) {
      if (activeAudio.readyState < 2) {
        activeAudio.load();
      }
      void activeAudio.play()
        .then(() => {
          if (!isCurrentPlaybackRequest(requestToken, song.id)) {
            if (activeAudio.dataset.songId === song.id) {
              resetAudioDeck(activeAudio);
            }
          }
        })
        .catch(() => {});
    } else if (!isSameSong && activeAudio.preload !== "auto") {
      activeAudio.preload = "auto";
    }
  };

  const primeSongAudio = (song) => {
    const activeAudio = getActiveAudio();
    if (!song || !activeAudio || isPlaying()) {
      return;
    }
    if (isSongLoadedInAnyDeck(song.id)) {
      return;
    }

    prefetchSongIds(getSongNeighborhoodIds(song));

    const version = encodeURIComponent(song.updatedAt || song.id);
    const nextRelativeUrl = `${song.audioUrl}?v=${version}`;
    const nextUrl = new URL(nextRelativeUrl, window.location.origin).href;
    if (activeAudio.src === nextUrl) {
      activeAudio.dataset.songId = song.id;
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

  const addSongToQueue = (song) => {
    if (!song?.id) {
      return;
    }
    if (queuedSongIds().has(song.id)) {
      setAccountMessage(`${song.track || "Song"} is already in queue`);
      return;
    }
    setPlayQueue((current) => [...current, makeQueueEntry(song.id)]);
    setAccountMessage(`${song.track || "Song"} added to queue`);
  };

  const addCurrentSongToQueue = () => {
    const song = currentSong();
    if (song) {
      addSongToQueue(song);
    }
  };

  const removeSongFromQueue = (identifier) => {
    setPlayQueue((current) => {
      const exactIndex = current.findIndex((entry) => queueEntryId(entry) === identifier);
      const index = exactIndex >= 0 ? exactIndex : current.findIndex((entry) => queueEntrySongId(entry) === identifier);
      if (index < 0) {
        return current;
      }
      return [...current.slice(0, index), ...current.slice(index + 1)];
    });
  };

  const clearPlaybackQueue = () => {
    setPlayQueue([]);
    setAccountMessage("Queue cleared");
  };

  const takeNextQueuedSong = () => {
    const entries = playQueue();
    const contextIds = entries.map(queueEntrySongId).filter(Boolean);
    for (const entry of entries) {
      const songId = queueEntrySongId(entry);
      const song = songIndex().get(songId);
      if (song) {
        removeSongFromQueue(queueEntryId(entry) || songId);
        return { song, contextIds };
      }
    }
    if (entries.length) {
      setPlayQueue([]);
    }
    return null;
  };

  const playQueuedSong = (song, options = {}) => {
    if (!song?.id) {
      return;
    }
    const queuedIds = playQueue().map(queueEntrySongId).filter(Boolean);
    const contextIds = queuedIds.includes(song.id) ? queuedIds : [song.id, ...queuedIds];
    removeSongFromQueue(song.queueEntryId || song.id);
    loadSong(song, true, {
      ...options,
      playbackContextSource: "queue",
      playbackContextIds: contextIds,
    });
  };

  const queuePlaybackContextIds = (fallbackIds = []) => {
    const context = playbackContext();
    return context.source === "queue" && context.songIds.length ? context.songIds : fallbackIds;
  };

  const playNextFromQueueOrRelative = (options = {}) => {
    if (!radioPlaybackLocked()) {
      const queued = takeNextQueuedSong();
      if (queued?.song) {
        loadSong(queued.song, true, {
          ...options,
          playbackContextSource: "queue",
          playbackContextIds: queuePlaybackContextIds(queued.contextIds),
        });
        return true;
      }
    }
    selectRelative(1, true, currentTrackId() || selectedId(), options);
    return false;
  };

  const moveSelection = (offset) => {
    const nextSong = pickRelativeSong(offset, selectedId(), { respectRandom: false });
    if (!nextSong) {
      return;
    }
    setSelectedId(nextSong.id);
  };

  const pickRelativeSong = (offset, baseId = selectedId(), options = {}) => {
    const { respectRandom = true, usePlaybackContext = false } = options;
    const contextSongs = playbackContextSongs();
    const list = usePlaybackContext && contextSongs.length ? contextSongs : sortedActiveSongList();
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
    const nextSong = pickRelativeSong(offset, baseId, {
      respectRandom: autoplay,
      usePlaybackContext: autoplay || options.usePlaybackContext,
    });
    if (!nextSong) {
      return;
    }
    if (autoplay) {
      loadSong(nextSong, true, { ...options, preservePlaybackContext: true });
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

    if (!currentSong() && sortedActiveSongList()[0]) {
      loadSong(mainTab() === "radio" ? sortedActiveSongList()[0] : (selectedActiveSong() || selectedSong() || sortedActiveSongList()[0]), true);
      return;
    }

    if (activeAudio.paused) {
      if (selectedActiveSong() && selectedActiveSong()?.id !== currentTrackId()) {
        loadSong(selectedActiveSong(), true);
        return;
      }
      if (!activeAudio.src) {
        const fallbackSong = mainTab() === "radio"
          ? currentSong() || sortedActiveSongList()[0]
          : selectedActiveSong() || selectedSong() || currentSong() || sortedActiveSongList()[0];
        if (fallbackSong) {
          loadSong(fallbackSong, true);
          return;
        }
      }
      void activeAudio.play().catch(() => {});
    } else {
      nextPlaybackRequestToken();
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
      if (query().trim()) {
        if (movieFilter() && !event.data.payload.songs.some((song) => albumIdentity(song) === movieFilter())) {
          setMovieFilter("");
          setAlbumFilterMeta(null);
        }
        if (artistFilter() && !event.data.payload.songs.some((song) => normalizeText(song.singers).includes(normalizeText(artistFilter())))) {
          setArtistFilter("");
        }
        if (musicDirectorFilter() && !event.data.payload.songs.some((song) => normalizeText(song.musicDirector) === normalizeText(musicDirectorFilter()))) {
          setMusicDirectorFilter("");
        }
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

      if (commandKey && ["1", "2", "3", "4", "5", "6", "7"].includes(event.key)) {
        event.preventDefault();
        if (event.key === "1") activateMainTabShortcut("library");
        if (event.key === "2") activateMainTabShortcut("recents");
        if (event.key === "3") activateMainTabShortcut("favorites");
        if (event.key === "4") activateMainTabShortcut("queue");
        if (event.key === "5") activateMainTabShortcut("playlists");
        if (event.key === "6" && radioEnabled()) activateMainTabShortcut("radio");
        if (event.key === "7") activateMainTabShortcut("admin");
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
        loadSong(selectedActiveSong() || selectedSong() || sortedActiveSongList()[0], true);
        return;
      }

      if (event.key === " ") {
        event.preventDefault();
        if (!isPlaying()) {
          loadSong(selectedActiveSong() || selectedSong() || sortedActiveSongList()[0], true);
        } else {
          togglePlayback();
        }
        return;
      }

      if (event.key === "[") {
        event.preventDefault();
        if (!radioPlaybackLocked()) {
          selectRelative(-1, true, currentTrackId() || selectedId(), { forceImmediate: true });
        }
        return;
      }

      if (event.key === "]") {
        event.preventDefault();
        if (!radioPlaybackLocked()) {
          playNextFromQueueOrRelative({ forceImmediate: true });
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

    const onResize = () => {
      if (window.innerWidth >= 640) {
        setShowMobileMenu(false);
      }
      if (window.innerWidth >= 768) {
        setShowMobilePlayerPanel(false);
        setMobilePlayerDragOffset(0);
      }
    };

    window.addEventListener("keydown", onKeyDown);
    window.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("resize", onResize);
    const onBrowserOnline = () => {
      void verifyAppOnline();
    };
    const onBrowserOffline = () => {
      markAppOffline("No internet connection detected.");
    };
    window.addEventListener("online", onBrowserOnline);
    window.addEventListener("offline", onBrowserOffline);
    removeKeydownListener = () => window.removeEventListener("keydown", onKeyDown);
    removePointerdownListener = () => window.removeEventListener("pointerdown", onPointerDown);
    removeOnlineListener = () => window.removeEventListener("online", onBrowserOnline);
    removeOfflineListener = () => window.removeEventListener("offline", onBrowserOffline);
    onCleanup(() => window.removeEventListener("resize", onResize));

    void verifyAppOnline();

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
        getActiveAudio().dataset.songId = initialSongs[0].id;
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
    if (typeof navigator === "undefined" || !("mediaSession" in navigator)) {
      return;
    }
    const mediaSession = navigator.mediaSession;
    const activeSong = currentSong() || selectedActiveSong() || selectedSong() || sortedActiveSongList()[0] || null;
    mediaSession.playbackState = isPlaying() ? "playing" : "paused";
    mediaSession.metadata = activeSong
      ? new MediaMetadata({
          title: activeSong.track || "isaibox",
          artist: activeSong.singers || activeSong.musicDirector || "isaibox",
          album: activeSong.movie || "",
        })
      : null;

    try {
      mediaSession.setActionHandler("play", () => {
        if (!isPlaying()) {
          loadSong(selectedActiveSong() || selectedSong() || sortedActiveSongList()[0], true);
        }
      });
      mediaSession.setActionHandler("pause", () => {
        if (isPlaying()) {
          togglePlayback();
        }
      });
      mediaSession.setActionHandler("previoustrack", () => {
        if (!radioPlaybackLocked()) {
          selectRelative(-1, true, currentTrackId() || selectedId(), { allowCrossfade: false });
        }
      });
      mediaSession.setActionHandler("nexttrack", () => {
        if (!radioPlaybackLocked()) {
          playNextFromQueueOrRelative({ allowCrossfade: false });
        }
      });
      mediaSession.setActionHandler("seekbackward", () => adjustSeek(-10));
      mediaSession.setActionHandler("seekforward", () => adjustSeek(10));
      mediaSession.setActionHandler("seekto", (details) => {
        if (radioPlaybackLocked()) {
          return;
        }
        const seekTime = Number(details?.seekTime);
        if (!Number.isFinite(seekTime)) {
          return;
        }
        const activeAudio = getActiveAudio();
        if (activeAudio) {
          activeAudio.currentTime = seekTime;
        }
        setCurrentTime(seekTime);
      });
    } catch {}
  });

  createEffect(() => {
    if (typeof navigator === "undefined" || !("mediaSession" in navigator) || !Number.isFinite(duration()) || duration() <= 0) {
      return;
    }
    try {
      navigator.mediaSession.setPositionState({
        duration: duration(),
        playbackRate: playbackSpeed(),
        position: Math.min(duration(), Math.max(0, currentTime())),
      });
    } catch {}
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
    clearInterval(healthPollTimer);
    if (!localMode()) {
      return;
    }
    void verifyAppOnline();
    healthPollTimer = setInterval(() => {
      void verifyAppOnline();
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
    if (!showMobilePlayerPanel()) {
      setMobilePlayerDragOffset(0);
      mobilePlayerTouchStartY = null;
    }
  });

  createEffect(() => {
    if (mainTab() !== "playlists") {
      setShowMobilePlaylistPicker(false);
    }
  });

  createEffect(() => {
    const list = sortedActiveSongList();
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
    const song = selectedActiveSong() || selectedSong() || sortedActiveSongList()[0];
    if (!song) {
      return;
    }
    primeSongAudio(song);
  });

  const scrollSongRowIntoView = (songId, options = {}) => {
    if (!songId || !listRef) {
      return false;
    }
    const row = rowRefs.get(songId);
    if (!row) {
      return false;
    }
    const listRect = listRef.getBoundingClientRect();
    const rowRect = row.getBoundingClientRect();
    const topPadding = 12;
    const bottomPadding = 12;

    if (options.center) {
      const nextScrollTop = listRef.scrollTop + (rowRect.top - listRect.top) - ((listRect.height - rowRect.height) / 2);
      animateListScroll(Math.max(0, nextScrollTop));
      return true;
    }

    if (rowRect.top < listRect.top + topPadding || rowRect.bottom > listRect.bottom - bottomPadding) {
      const nextScrollTop = rowRect.top < listRect.top + topPadding
        ? listRef.scrollTop - ((listRect.top + topPadding) - rowRect.top)
        : listRef.scrollTop + (rowRect.bottom - (listRect.bottom - bottomPadding));
      animateListScroll(Math.max(0, nextScrollTop));
    }
    return true;
  };

  createEffect(() => {
    const currentId = selectedId();
    if (!currentId) {
      return;
    }
    scrollSongRowIntoView(currentId);
  });

  createEffect(() => {
    const targetId = pendingScrollSongId();
    if (!targetId) {
      return;
    }
    sortedActiveSongList();
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        if (scrollSongRowIntoView(targetId, { center: true })) {
          setPendingScrollSongId("");
        }
      });
    });
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
    nextPlaybackRequestToken();
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
    nextPlaybackRequestToken();
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
            onClick={() => { markAppOnline(); }}
            class="mt-2 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--fg)] underline transition hover:text-[var(--soft)]"
          >
            Dismiss
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
      <Show when={loading() && !songs().length}>
        <div class="absolute inset-0 z-[70] flex items-center justify-center bg-[var(--bg)] px-8 text-center">
          <div>
            <div class="font-mono text-[11px] uppercase tracking-[0.32em] text-[var(--brand)]">isaibox</div>
            <div class="mt-4 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)]">
              Loading library{loadingDots()}
            </div>
          </div>
        </div>
      </Show>
      <header class="flex min-w-0 flex-wrap items-center gap-3 border-b border-[var(--line)] px-4 py-3 sm:px-6 sm:py-4 sm:flex-nowrap sm:justify-between">
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
        <div class="ml-auto flex min-w-0 flex-wrap items-center justify-end gap-2 md:gap-3">
          <div
            ref={(el) => {
              googleButtonRef = el;
            }}
            aria-hidden="true"
            class="pointer-events-none absolute left-[-9999px] top-0 opacity-0"
          />
          <div class="hidden items-center gap-3 sm:flex sm:gap-4">
            <button
              type="button"
              onClick={() => setShowSettings(!showSettings())}
              class={`font-mono text-[10px] uppercase tracking-[0.22em] transition ${
                showSettings() ? "text-[var(--fg)] underline underline-offset-4" : "text-[var(--soft)] hover:text-[var(--fg)]"
              }`}
            >
              Settings
            </button>
            <button
              type="button"
              onClick={() => setShowShortcutHelp(true)}
              class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
            >
              Shortcuts
            </button>
          </div>
          <button
            type="button"
            onClick={() => setShowMobileMenu(true)}
            aria-label="Open menu"
            class="flex h-10 w-10 items-center justify-center rounded-full border border-[var(--line)] text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)] sm:hidden"
          >
            <HamburgerIcon />
          </button>

          <Show when={localMode()}>
            <div class="hidden items-center gap-3 sm:flex">
              <Show when={cacheNearFull() && !cacheFull()}>
                <span class="font-mono text-[10px] uppercase tracking-[0.18em] text-yellow-500">
                  Cache {cachePercent()}%
                </span>
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
                  class={`flex h-9 items-center gap-2 rounded-full border px-4 text-[var(--soft)] transition ${
                    showProfileMenu() ? "border-[var(--fg)] bg-[var(--hover)] text-[var(--fg)]" : "border-[var(--line)] hover:border-[var(--fg)] hover:text-[var(--fg)]"
                  }`}
                >
                  <span class="text-sm">Account</span>
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

      <Show when={showMobileMenu()}>
        <div class="fixed inset-0 z-50 bg-black/55 sm:hidden">
          <button
            type="button"
            aria-label="Close mobile menu"
            class="absolute inset-0"
            onClick={() => setShowMobileMenu(false)}
          />
          <div class="absolute right-0 top-0 h-full w-[min(88vw,340px)] border-l border-[var(--line)] bg-[var(--bg)] px-5 py-5 shadow-2xl">
            <div class="flex items-center justify-between gap-4">
              <div>
                <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Menu</div>
                <div class="mt-2 text-lg font-semibold">Mobile controls</div>
              </div>
              <button
                type="button"
                onClick={() => setShowMobileMenu(false)}
                class="rounded-full border border-[var(--line)] px-3 py-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]"
              >
                Close
              </button>
            </div>

            <div class="mt-5 space-y-4">
              <div class="rounded-[18px] border border-[var(--line)] px-4 py-4">
                <div class="font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--faint)]">Theme</div>
                <div class="mt-3 grid grid-cols-3 gap-2">
                  <button type="button" onClick={() => setThemePreferenceChoice("light")} class={`border px-2 py-3 font-mono text-[10px] uppercase tracking-[0.16em] transition ${themePreference() === "light" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)] text-[var(--soft)]"}`}>Light</button>
                  <button type="button" onClick={() => setThemePreferenceChoice("system")} class={`border px-2 py-3 font-mono text-[10px] uppercase tracking-[0.16em] transition ${themePreference() === "system" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)] text-[var(--soft)]"}`}>System</button>
                  <button type="button" onClick={() => setThemePreferenceChoice("dark")} class={`border px-2 py-3 font-mono text-[10px] uppercase tracking-[0.16em] transition ${themePreference() === "dark" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)] text-[var(--soft)]"}`}>Dark</button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </Show>

      <Show when={showSettings()}>
        <section class="hidden border-b border-[var(--line)] bg-[var(--panel)] px-6 py-4 sm:block">
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
              <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Appearance</div>
              <div class="mt-3 flex flex-wrap items-center gap-3">
                <button
                  type="button"
                  onClick={() => setThemePreferenceChoice("light")}
                  class={`border px-4 py-2 font-mono text-[10px] uppercase tracking-[0.16em] transition ${
                    themePreference() === "light"
                      ? "border-[var(--fg)] bg-[var(--hover)] text-[var(--fg)]"
                      : "border-[var(--line)] text-[var(--soft)] hover:border(--fg)] hover:text-[var(--fg)]"
                  }`}
                >
                  Light
                </button>
                <button
                  type="button"
                  onClick={() => setThemePreferenceChoice("dark")}
                  class={`border px-4 py-2 font-mono text-[10px] uppercase tracking-[0.16em] transition ${
                    themePreference() === "dark"
                      ? "border-[var(--fg)] bg-[var(--hover)] text-[var(--fg)]"
                      : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"
                  }`}
                >
                  Dark
                </button>
                <button
                  type="button"
                  onClick={() => setThemePreferenceChoice("system")}
                  class={`border px-4 py-2 font-mono text-[10px] uppercase tracking-[0.16em] transition ${
                    themePreference() === "system"
                      ? "border-[var(--fg)] bg-[var(--hover)] text-[var(--fg)]"
                      : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"
                  }`}
                >
                  System (Auto)
                </button>
              </div>
            </div>

            <Show when={localMode()}>
              <div class="border border-[var(--line)] p-4">
                <div class="flex flex-wrap items-start justify-between gap-4">
                  <div>
                    <div class="text-sm font-semibold">Library updates</div>
                    <div class={`mt-1 font-mono text-[10px] uppercase tracking-[0.18em] ${localDbSyncTone()}`}>
                      {localDbSyncLabel() || "Library updates unavailable"}
                    </div>
                    <Show when={dbSyncState()?.message}>
                      <div class="mt-2 max-w-2xl text-sm text-[var(--soft)]">{dbSyncState().message}</div>
                    </Show>
                    <Show when={dbSyncActionMessage() && dbSyncActionMessage() !== dbSyncState()?.message}>
                      <div class={`mt-2 max-w-2xl text-sm ${localDbSyncTone()}`}>{dbSyncActionMessage()}</div>
                    </Show>
                  </div>
                  <div class="flex flex-wrap items-center gap-3">
                    <button
                      type="button"
                      onClick={() => void checkForLibraryUpdate()}
                      disabled={dbSyncActionBusy() || dbSyncState()?.status === "downloading"}
                      class="border border-[var(--line)] px-3 py-2 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)] disabled:opacity-50"
                    >
                      {localDbSyncActionLabel()}
                    </button>
                    <Show when={dbSyncState()?.status === "error" && dbSyncState()?.githubIssuesUrl}>
                      <a
                        href={dbSyncState().githubIssuesUrl}
                        target="_blank"
                        rel="noreferrer"
                        class="border border-[var(--line)] px-3 py-2 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)]"
                      >
                        Raise issue
                      </a>
                    </Show>
                  </div>
                </div>
              </div>

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
              </div>
            </Show>
          </div>
        </section>
      </Show>

      <section class="shrink-0 border-b border-[var(--line)] px-4 py-3 sm:px-6">
        <div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div class="flex gap-2 overflow-x-auto pb-1 font-mono text-[10px] uppercase tracking-[0.22em] [-ms-overflow-style:none] [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
            <button type="button" onClick={() => setMainBrowseTab("library")} class={`shrink-0 rounded-full border px-3 py-2 transition sm:border-transparent sm:px-1 sm:py-1 ${mainTab() === "library" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)] sm:bg-transparent sm:text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>
              Library
            </button>
            <Show when={localMode()}>
              <button type="button" onClick={() => setMainBrowseTab("playlists")} class={`shrink-0 rounded-full border px-3 py-2 transition sm:border-transparent sm:px-1 sm:py-1 ${mainTab() === "playlists" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)] sm:bg-transparent sm:text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>
                Playlists {playlists().length ? `(${playlists().length})` : ""}
              </button>
            </Show>
            <button type="button" onClick={() => setMainBrowseTab("queue")} class={`shrink-0 rounded-full border px-3 py-2 transition sm:border-transparent sm:px-1 sm:py-1 ${mainTab() === "queue" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)] sm:bg-transparent sm:text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>
              Queue {playQueue().length ? `(${playQueue().length})` : ""}
            </button>
            <button type="button" onClick={() => setMainBrowseTab("recents")} class={`shrink-0 rounded-full border px-3 py-2 transition sm:border-transparent sm:px-1 sm:py-1 ${mainTab() === "recents" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)] sm:bg-transparent sm:text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>
              Recents {recentSongs().length ? `(${recentSongs().length})` : ""}
            </button>
            <Show when={libraryProfileEnabled()}>
              <button type="button" onClick={() => setMainBrowseTab("favorites")} class={`shrink-0 rounded-full border px-3 py-2 transition sm:border-transparent sm:px-1 sm:py-1 ${mainTab() === "favorites" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)] sm:bg-transparent sm:text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>
                Favorites
              </button>
            </Show>
            <Show when={radioEnabled()}>
              <button type="button" onClick={() => setMainBrowseTab("radio")} class={`shrink-0 rounded-full border px-3 py-2 transition sm:border-transparent sm:px-1 sm:py-1 ${mainTab() === "radio" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)] sm:bg-transparent sm:text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>
                Radio
              </button>
            </Show>
            <Show when={authEnabled() && user()?.is_admin}>
              <button type="button" onClick={() => setMainBrowseTab("admin")} class={`shrink-0 rounded-full border px-3 py-2 transition sm:border-transparent sm:px-1 sm:py-1 ${mainTab() === "admin" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)] sm:bg-transparent sm:text-[var(--fg)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}>
                Admin
              </button>
            </Show>
          </div>
          <div class="flex min-w-0 flex-1 items-center gap-3 lg:min-w-[280px] lg:justify-end">
            <Show
              when={mainTab() === "library"}
              fallback={
                <div
                  class={`w-full flex-wrap items-center gap-3 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] lg:max-w-[560px] lg:justify-end ${
                    mainTab() === "recents"
                      ? "flex min-h-[42px]"
                      : mainTab() === "favorites"
                        ? "hidden sm:flex sm:min-h-[42px]"
                        : "hidden"
                  }`}
                >
                  <Show when={mainTab() === "recents"}>
                    <>
                      <span>{recentSongs().length} tracks</span>
                      <Show when={recentSongs().length > 0}>
                        <button type="button" onClick={clearRecents} class="transition hover:text-[var(--fg)]">Clear recents</button>
                      </Show>
                    </>
                  </Show>
                  <Show when={mainTab() === "favorites" && libraryProfileEnabled()}>
                    <>
                      <div class="hidden items-center gap-3 sm:flex">
                        <button
                          type="button"
                          onClick={() => setFavoritesTab("songs")}
                          class={`transition ${favoritesTab() === "songs" ? "text-[var(--fg)]" : "hover:text-[var(--fg)]"}`}
                        >
                          Songs ({favoriteSongs().length})
                        </button>
                        <button
                          type="button"
                          onClick={() => setFavoritesTab("albums")}
                          class={`transition ${favoritesTab() === "albums" ? "text-[var(--fg)]" : "hover:text-[var(--fg)]"}`}
                        >
                          Albums ({favoriteAlbumRows().length})
                        </button>
                        <button
                          type="button"
                          onClick={() => setFavoritesTab("music-directors")}
                          class={`transition ${favoritesTab() === "music-directors" ? "text-[var(--fg)]" : "hover:text-[var(--fg)]"}`}
                        >
                          Music Directors ({favoriteMusicDirectorRows().length})
                        </button>
                      </div>
                    </>
                  </Show>
                </div>
              }
            >
              <div class="flex h-[42px] w-full items-center gap-3 border border-[var(--line)] px-3 lg:max-w-[560px]">
                <span class="font-mono text-sm text-[var(--soft)]">/</span>
                <input
                  ref={(el) => {
                    searchInputRef = el;
                  }}
                  value={query()}
                  onInput={(event) => {
                    setMovieFilter("");
                    setAlbumFilterMeta(null);
                    setArtistFilter("");
                    setMusicDirectorFilter("");
                    setQuery(event.currentTarget.value);
                  }}
                  placeholder={
                    searchTab() === "albums"
                      ? "Search albums, composers, years..."
                      : searchTab() === "music-directors"
                        ? "Search music directors, albums, years..."
                        : "Search tracks, singers, albums..."
                  }
                  class="w-full bg-transparent font-mono text-sm tracking-wide text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                />
                <button
                  type="button"
                  onClick={() => {
                    setMovieFilter("");
                    setAlbumFilterMeta(null);
                    setArtistFilter("");
                    setMusicDirectorFilter("");
                    setQuery("");
                  }}
                  class="shrink-0 font-mono text-xs uppercase tracking-[0.2em] text-[var(--muted)] transition hover:text-[var(--fg)]"
                >
                  Clear
                </button>
              </div>
            </Show>
          </div>
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
                  onInput={(event) => setGlobalPlaylistNameInput(sanitizePlaylistName(event.currentTarget.value))}
                  placeholder="New global playlist"
                  maxLength={PLAYLIST_NAME_MAX_LENGTH}
                  class="min-w-[180px] bg-transparent font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                />
                <button
                  type="button"
                  onClick={() => void createGlobalPlaylist()}
                  disabled={playlistMutationBusy() === "create-global"}
                  class={`font-mono text-[10px] uppercase tracking-[0.22em] transition ${
                    playlistMutationBusy() === "create-global" ? "cursor-not-allowed text-[var(--line)]" : "text-[var(--soft)] hover:text-[var(--fg)]"
                  }`}
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
                    onInput={(event) => setRadioSaveName(sanitizePlaylistName(event.currentTarget.value))}
                    placeholder="Playlist name"
                    maxLength={PLAYLIST_NAME_MAX_LENGTH}
                    class="min-w-[180px] bg-transparent font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                  />
                  <button
                    type="button"
                    onClick={() => void saveRadioStationWithMode()}
                    disabled={playlistMutationBusy() === "radio-save"}
                    class={`font-mono text-[10px] uppercase tracking-[0.22em] transition ${
                      playlistMutationBusy() === "radio-save" ? "cursor-not-allowed text-[var(--line)]" : "text-[var(--soft)] hover:text-[var(--fg)]"
                    }`}
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
                              onInput={(event) => setGlobalPlaylistNameEdit(sanitizePlaylistName(event.currentTarget.value))}
                              maxLength={PLAYLIST_NAME_MAX_LENGTH}
                              class="min-w-[180px] bg-transparent font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                            />
                            <button
                              type="button"
                              onClick={() => void renameGlobalPlaylist()}
                              disabled={playlistMutationBusy() === `rename:${playlist().id}`}
                              class={`font-mono text-[10px] uppercase tracking-[0.2em] transition ${
                                playlistMutationBusy() === `rename:${playlist().id}` ? "cursor-not-allowed text-[var(--line)]" : "text-[var(--soft)] hover:text-[var(--fg)]"
                              }`}
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
                  <div class="flex items-center justify-between gap-4"><span>Queue</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl+4</span></div>
                  <Show when={localMode()}>
                    <div class="flex items-center justify-between gap-4"><span>Playlists</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl+5</span></div>
                  </Show>
                  <div class="flex items-center justify-between gap-4"><span>Radio</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl+6 or R</span></div>
                  <Show when={authEnabled() && user()?.is_admin}>
                    <div class="flex items-center justify-between gap-4"><span>Admin</span><span class="font-mono text-[11px] text-[var(--soft)]">Cmd/Ctrl+7</span></div>
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
        <Show when={mainTab() === "radio"}>
          <section class="border-b border-[var(--line-soft)] px-6 py-4">
            <div class="flex flex-wrap items-center justify-between gap-3">
              <div>
                <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Radio station</div>
                <div class="mt-1 text-sm text-[var(--soft)]">
                  {currentRadioStation()?.blurb || "Gemini-sorted stations with looping 100-song queues."}
                </div>
              </div>
              <div class="flex flex-wrap items-center gap-3 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)]">
                <span>{activeSongList().length} tracks</span>
                <button type="button" onClick={startRadio} class="transition hover:text-[var(--fg)]">Start radio</button>
                <button type="button" onClick={() => void fetchRadioStations(true)} class="transition hover:text-[var(--fg)]">
                  Refresh stations
                </button>
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
                      <button
                        type="button"
                        onClick={() => openCurrentSongAlbum(song())}
                        class="mt-3 text-left text-xl font-semibold transition hover:text-[var(--soft)]"
                      >
                        {song().track}
                      </button>
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
            const selectedPlaylistDetail = createMemo(() => globalPlaylistDetail());
            const myPlaylistDetail = createMemo(() => {
              const detail = selectedPlaylistDetail();
              if (!detail) return null;
              return userPlaylistIds().has(detail.id) ? detail : null;
            });
            return (
              <>
              <section class="min-h-0 flex-1 overflow-hidden px-4 py-3 sm:px-6 sm:py-4">
                <div class="grid h-full min-h-0 gap-3 xl:grid-cols-[280px_minmax(0,1fr)] xl:gap-4">
                  <aside class="hidden min-h-0 overflow-y-auto border border-[var(--line)] bg-[var(--panel)] p-4 xl:block xl:max-h-none">
                    <div class="flex items-center gap-2">
                      <input
                        value={playlistNameInput()}
                        onInput={(event) => setPlaylistNameInput(sanitizePlaylistName(event.currentTarget.value))}
                        onKeyDown={(event) => { if (event.key === "Enter") void createPlaylist(); }}
                        placeholder="New playlist name"
                        maxLength={PLAYLIST_NAME_MAX_LENGTH}
                        class="min-w-0 flex-1 border border-[var(--line)] bg-transparent px-3 py-2 font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                      />
                      <button
                        type="button"
                        onClick={() => void createPlaylist()}
                        disabled={playlistCreateBusy()}
                        class={`border px-3 py-2 font-mono text-[10px] uppercase tracking-[0.2em] transition ${
                          playlistCreateBusy() ? "cursor-not-allowed border-[var(--line-soft)] text-[var(--line)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"
                        }`}
                      >
                        +
                      </button>
                    </div>

                    <div class="mt-4 flex gap-2 overflow-x-auto font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]">
                      <button
                        type="button"
                        onClick={() => setPlaylistBrowseSection("global")}
                        class={`shrink-0 rounded-full border px-3 py-2 transition ${playlistBrowseSection() === "global" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}
                      >
                        Global {globalPlaylists().length ? `(${globalPlaylists().length})` : ""}
                      </button>
                      <button
                        type="button"
                        onClick={() => setPlaylistBrowseSection("yours")}
                        class={`shrink-0 rounded-full border px-3 py-2 transition ${playlistBrowseSection() === "yours" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}
                      >
                        Yours {playlists().length ? `(${playlists().length})` : ""}
                      </button>
                    </div>

                    <div class="mt-3">
                      <input
                        value={playlistSearchQuery()}
                        onInput={(event) => setPlaylistSearchQuery(event.currentTarget.value)}
                        placeholder="Search playlists"
                        class="w-full border border-[var(--line)] bg-transparent px-3 py-2 font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                      />
                    </div>

                    <div class="mt-4 space-y-1">
                      <Show when={playlistBrowseSection() === "global"}>
                        <For each={filteredGlobalPlaylists()}>
                          {(playlist) => (
                            <button
                              type="button"
                              onClick={() => void openGlobalPlaylist(playlist.id)}
                              class={`flex w-full items-center justify-between gap-2 border px-3 py-3 text-left transition ${
                                selectedPlaylistDetail()?.id === playlist.id
                                  ? "border-[var(--fg)] bg-[var(--hover)]"
                                  : "border-[var(--line)] hover:border-[var(--fg)]"
                              }`}
                            >
                              <span class="min-w-0 truncate text-sm">{playlist.name}</span>
                              <span class="shrink-0 font-mono text-[10px] text-[var(--soft)]">{playlist.trackCount}</span>
                            </button>
                          )}
                        </For>
                        <Show when={filteredGlobalPlaylists().length === 0}>
                          <div class="px-1 py-3 text-sm text-[var(--soft)]">
                            {normalizedPlaylistSearch() ? "No global playlists match." : "Global playlists are still loading."}
                          </div>
                        </Show>
                      </Show>

                      <Show when={playlistBrowseSection() === "yours"}>
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
                        <Show when={filteredUserPlaylists().length === 0}>
                          <div class="px-1 py-3 text-sm text-[var(--soft)]">
                            {normalizedPlaylistSearch() ? "No personal playlists match." : "Create a personal playlist when needed."}
                          </div>
                        </Show>
                      </Show>
                    </div>
                  </aside>

                  <div class="order-2 flex min-h-0 flex-col overflow-y-auto border border-[var(--line)] bg-[var(--panel)] xl:order-2 xl:overflow-hidden">
                    <div class="mobile-section-pad border-b border-[var(--line-soft)] xl:hidden">
                      <Show
                        when={showMobilePlaylistPicker()}
                        fallback={
                          <>
                            <div class="mobile-card flex items-start justify-between gap-3">
                              <div class="min-w-0">
                                <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Playlist</div>
                                <Show when={selectedPlaylistDetail()} fallback={<div class="mt-2 text-lg font-semibold">No playlist selected</div>}>
                                  {(playlist) => (
                                    <>
                                      <div class="mt-2 truncate text-lg font-semibold">{playlist().name}</div>
                                      <div class="mt-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]">
                                        {(playlist().tracks || []).length} tracks
                                      </div>
                                    </>
                                  )}
                                </Show>
                              </div>
                              <button
                                type="button"
                                onClick={() => setShowMobilePlaylistPicker(true)}
                                class="shrink-0 rounded-full border border-[var(--line)] px-4 py-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]"
                              >
                                Playlists
                              </button>
                            </div>
                          </>
                        }
                      >
                        {() => (
                          <>
                            <button
                              type="button"
                              onClick={() => {
                                setShowMobilePlaylistPicker(false);
                              }}
                              class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                            >
                              ← Back
                            </button>
                            <div class="mt-3 flex gap-2 overflow-x-auto font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]">
                              <button
                                type="button"
                                onClick={() => setMobilePlaylistSection("global")}
                                class={`shrink-0 rounded-full border px-4 py-2 transition ${mobilePlaylistSection() === "global" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)]"}`}
                              >
                                Global
                              </button>
                              <button
                                type="button"
                                onClick={() => setMobilePlaylistSection("yours")}
                                class={`shrink-0 rounded-full border px-4 py-2 transition ${mobilePlaylistSection() === "yours" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)]"}`}
                              >
                                Yours
                              </button>
                            </div>
                            <Show when={mobilePlaylistSection() === "yours"}>
                              <div class="mt-4 flex items-center gap-2">
                                <input
                                  value={playlistNameInput()}
                                  onInput={(event) => setPlaylistNameInput(sanitizePlaylistName(event.currentTarget.value))}
                                  onKeyDown={(event) => { if (event.key === "Enter") void createPlaylist(); }}
                                  placeholder="New playlist name"
                                  maxLength={PLAYLIST_NAME_MAX_LENGTH}
                                  class="min-w-0 flex-1 rounded-full border border-[var(--line)] bg-transparent px-4 py-3 font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                                />
                                <button
                                  type="button"
                                  onClick={() => void createPlaylist()}
                                  disabled={playlistCreateBusy()}
                                  class={`rounded-full border px-4 py-3 font-mono text-[10px] uppercase tracking-[0.2em] transition ${
                                    playlistCreateBusy() ? "cursor-not-allowed border-[var(--line-soft)] text-[var(--line)]" : "border-[var(--line)] text-[var(--soft)]"
                                  }`}
                                >
                                  Add
                                </button>
                              </div>
                            </Show>
                            <div class="mt-3">
                              <input
                                value={playlistSearchQuery()}
                                onInput={(event) => setPlaylistSearchQuery(event.currentTarget.value)}
                                placeholder="Search playlists"
                                class="w-full rounded-full border border-[var(--line)] bg-transparent px-4 py-3 font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                              />
                            </div>
                            <div class="mt-4 space-y-4">
                              <Show when={mobilePlaylistSection() === "yours" && filteredUserPlaylists().length > 0}>
                                <div>
                                  <div class="mb-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--faint)]">Yours</div>
                                  <div class="space-y-2">
                                    <For each={filteredUserPlaylists()}>
                                      {(playlist) => (
                                        <button
                                          type="button"
                                          onClick={() => {
                                            setShowMobilePlaylistPicker(false);
                                            void openGlobalPlaylist(playlist.id);
                                          }}
                                          class="mobile-list-row flex w-full items-center justify-between gap-3 text-left text-[var(--fg)] transition"
                                        >
                                          <div class="min-w-0">
                                            <div class="truncate text-sm font-semibold">{playlist.name}</div>
                                            <div class="mt-1 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]">{playlist.trackCount} tracks</div>
                                          </div>
                                          <div class="shrink-0 text-[var(--soft)]">
                                            <ChevronDownIcon />
                                          </div>
                                        </button>
                                      )}
                                    </For>
                                  </div>
                                </div>
                              </Show>
                              <Show when={mobilePlaylistSection() === "global" && filteredGlobalPlaylists().length > 0}>
                                <div>
                                  <div class="mb-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--faint)]">Global</div>
                                  <div class="space-y-2">
                                    <For each={filteredGlobalPlaylists()}>
                                      {(playlist) => (
                                        <button
                                          type="button"
                                          onClick={() => {
                                            setShowMobilePlaylistPicker(false);
                                            void openGlobalPlaylist(playlist.id);
                                          }}
                                          class="mobile-list-row flex w-full items-center justify-between gap-3 text-left text-[var(--fg)] transition"
                                        >
                                          <div class="min-w-0">
                                            <div class="truncate text-sm font-semibold">{playlist.name}</div>
                                            <div class="mt-1 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]">{playlist.trackCount} tracks</div>
                                          </div>
                                          <div class="shrink-0 text-[var(--soft)]">
                                            <ChevronDownIcon />
                                          </div>
                                        </button>
                                      )}
                                    </For>
                                  </div>
                                </div>
                              </Show>
                              <Show when={!normalizedPlaylistSearch() && ((mobilePlaylistSection() === "yours" && filteredUserPlaylists().length === 0) || (mobilePlaylistSection() === "global" && filteredGlobalPlaylists().length === 0))}>
                                <div class="mobile-card text-sm text-[var(--soft)]">
                                  No playlists yet.
                                </div>
                              </Show>
                              <Show when={normalizedPlaylistSearch() && ((mobilePlaylistSection() === "yours" && filteredUserPlaylists().length === 0) || (mobilePlaylistSection() === "global" && filteredGlobalPlaylists().length === 0))}>
                                <div class="mobile-card text-sm text-[var(--soft)]">
                                  No playlists match that search.
                                </div>
                              </Show>
                            </div>
                          </>
                        )}
                      </Show>
                    </div>
                    <Show when={playlistDetailLoading() && !selectedPlaylistDetail() && !showMobilePlaylistPicker()}>
                      <div class="flex min-h-0 flex-1 items-center justify-center px-6 text-center">
                        <div>
                          <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Playlist</div>
                          <div class="mt-3 text-sm text-[var(--soft)]">Loading{loadingDots()}</div>
                        </div>
                      </div>
                    </Show>
                    <Show when={selectedPlaylistDetail() && !showMobilePlaylistPicker()}>
                      {(playlist) => (
                        <>
                          <section class="hidden border-b border-[var(--line-soft)] px-4 py-4 xl:block">
                            <div class="flex items-center justify-between gap-4">
                              <div class="min-w-0">
                                <div class="mt-1 text-lg font-semibold">{playlist().name}</div>
                                <div class="mt-1 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)]">
                                  {(playlist().tracks || []).length} tracks
                                </div>
                              </div>
                              <Show when={myPlaylistDetail()}>
                                <div class="flex flex-wrap items-center gap-3 font-mono text-[10px] uppercase tracking-[0.2em] text-[var(--soft)]">
                                  <input
                                    value={globalPlaylistNameEdit()}
                                    onInput={(event) => setGlobalPlaylistNameEdit(sanitizePlaylistName(event.currentTarget.value))}
                                    onKeyDown={(event) => { if (event.key === "Enter") void renamePlaylistLocal(); }}
                                    placeholder="Rename"
                                    maxLength={PLAYLIST_NAME_MAX_LENGTH}
                                    class="min-w-[120px] border border-[var(--line)] bg-transparent px-2 py-1 font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                                  />
                                  <button
                                    type="button"
                                    onClick={() => void renamePlaylistLocal()}
                                    disabled={playlistMutationBusy() === `rename:${playlist().id}`}
                                    class={`transition ${
                                      playlistMutationBusy() === `rename:${playlist().id}` ? "cursor-not-allowed text-[var(--line)]" : "hover:text-[var(--fg)]"
                                    }`}
                                  >
                                    Rename
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => void clearVisiblePlaylist()}
                                    disabled={playlistMutationBusy() === `clear:${playlist().id}`}
                                    class={`transition ${
                                      playlistMutationBusy() === `clear:${playlist().id}` ? "cursor-not-allowed text-[var(--line)]" : "hover:text-[var(--fg)]"
                                    }`}
                                  >
                                    Clear
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => void deleteVisiblePlaylist()}
                                    disabled={playlistMutationBusy() === `delete:${playlist().id}`}
                                    class={`transition ${
                                      playlistMutationBusy() === `delete:${playlist().id}` ? "cursor-not-allowed text-[var(--line)]" : "hover:text-[var(--fg)]"
                                    }`}
                                  >
                                    Delete
                                  </button>
                                </div>
                              </Show>
                            </div>
                          </section>
                          <Show
                            when={sortedActiveSongList().length > 0}
                            fallback={
                              <div class="flex min-h-0 flex-1 items-center justify-center px-6 py-8 text-center xl:py-0">
                                <div class="max-w-md">
                                  <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Playlist Ready</div>
                                  <div class="mt-3 text-sm text-[var(--soft)]">
                                    {(playlist().tracks || []).length > 0 ? "No songs available in the current filter." : "Empty playlist"}
                                  </div>
                                </div>
                              </div>
                            }
                          >
                            <ul ref={listRef} class="min-h-0 flex-1 overflow-y-auto px-2">
                              <For each={sortedActiveSongList()}>
                                {(song, index) => {
                                  const active = () => selectedSong()?.id === song.id;
                                  return (
                                    <li>
                                      <button
                                        ref={(el) => {
                                          if (el) rowRefs.set(song.id, el);
                                          else rowRefs.delete(song.id);
                                        }}
                                        type="button"
                                        onClick={() => loadSong(song, true)}
                                        class={`song-table-row flex w-full flex-wrap items-start gap-x-3 gap-y-2 px-4 py-3 text-left transition ${
                                          active()
                                            ? currentTrackId() === song.id
                                              ? "song-row-active text-[var(--fg)]"
                                              : "bg-[var(--hover)] text-[var(--fg)]"
                                            : "bg-transparent text-[var(--fg)] hover:bg-[var(--hover)]"
                                        }`}
                                      >
                                        <span class="song-col-index w-8 shrink-0 pt-0.5 text-right font-mono text-xs text-[var(--soft)] sm:pt-0">
                                          {currentTrackId() === song.id && isPlaying() && streamStarted() ? <PlayingBars /> : String(index() + 1).padStart(2, "0")}
                                        </span>
                                        <div class="song-col-title min-w-0 flex-1 basis-[calc(100%-5.75rem)] sm:basis-auto">
                                          <div class="truncate text-sm">{song.track}</div>
                                          <div class="mt-1 flex flex-wrap gap-x-2 gap-y-1 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--soft)] sm:hidden">
                                            <Show when={song.movie}><span>{song.movie}</span></Show>
                                            <Show when={song.musicDirector}><span>{song.musicDirector}</span></Show>
                                            <Show when={song.singers}><span>{song.singers}</span></Show>
                                            <Show when={song.year}><span>{song.year}</span></Show>
                                          </div>
                                        </div>
                                        <DrilldownText value={song.movie} payload={{ album: song.movie, albumUrl: song.albumUrl, year: song.year }} onClick={navigateToMovie} class="song-col-movie hidden font-mono text-[11px] text-[var(--soft)] md:block" />
                                        <DrilldownText value={song.musicDirector} onClick={navigateToMusicDirector} class="song-col-director hidden font-mono text-[11px] text-[var(--soft)] lg:block" />
                                        <span class="song-col-singers hidden truncate font-mono text-[11px] text-[var(--soft)] xl:block">{song.singers || "-"}</span>
                                        <span class="song-col-year hidden font-mono text-[11px] text-[var(--soft)] sm:block">{song.year || "-"}</span>
                                        <SongRowActions
                                          queued={queuedSongIds().has(song.id)}
                                          showFavorite={user()}
                                          favorite={favoriteIdSet().has(song.id)}
                                          onQueue={() => addSongToQueue(song)}
                                          onFavorite={() => void toggleFavorite(song.id)}
                                        />
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
                    <Show when={!playlistDetailLoading() && !selectedPlaylistDetail() && !showMobilePlaylistPicker()}>
                      <div class="hidden min-h-0 flex-1 items-center justify-center px-6 text-center xl:flex">
                        <div class="text-sm text-[var(--soft)]">
                          {playlists().length > 0 ? "Select a playlist to manage it. Add songs from Library search or from another playlist." : "Create a playlist to get started."}
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
          <section class="min-h-0 flex-1 overflow-hidden px-4 py-3 sm:px-6 sm:py-4">
            <div class="grid h-full min-h-0 gap-3 xl:grid-cols-[320px_minmax(0,1fr)] xl:gap-4">
              <aside class="hidden min-h-0 max-h-[34vh] overflow-y-auto border border-[var(--line)] bg-[var(--panel)] p-4 xl:block xl:max-h-none">
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
                      onInput={(event) => setPlaylistNameInput(sanitizePlaylistName(event.currentTarget.value))}
                      placeholder="New playlist"
                      maxLength={PLAYLIST_NAME_MAX_LENGTH}
                      class="min-w-0 flex-1 border border-[var(--line)] bg-transparent px-3 py-2 font-mono text-xs text-[var(--fg)] outline-none placeholder:text-[var(--muted)]"
                    />
                    <button
                      type="button"
                      onClick={() => void createPlaylist()}
                      disabled={playlistCreateBusy()}
                      class={`border px-3 py-2 font-mono text-[10px] uppercase tracking-[0.2em] transition ${
                        playlistCreateBusy() ? "cursor-not-allowed border-[var(--line-soft)] text-[var(--line)]" : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"
                      }`}
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

              <div class="order-1 flex min-h-0 flex-col overflow-hidden border border-[var(--line)] bg-[var(--panel)] xl:order-2">
                <Show when={!playlistDetailLoading() && !playlistDetailError()}>
                  <div class="flex min-h-0 flex-1 flex-col overflow-hidden">
                    <header
                      class={`flex flex-wrap items-center gap-3 border-b border-[var(--line-soft)] px-4 py-4 sm:px-6 ${
                        visiblePlaylistDetail() && !movieFilter() && !artistFilter() && !musicDirectorFilter()
                          ? "hidden xl:flex"
                          : ""
                      }`}
                    >
                      <Show when={libraryNavStack().length > 0}>
                        <button
                          type="button"
                          onClick={restorePreviousLibraryView}
                          class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                        >
                          ← Back
                        </button>
                      </Show>
                      <div class="min-w-0 flex-1">
                        <Show when={visiblePlaylistDetail()}>
                          {(playlist) => (
                            <div class="hidden xl:block">
                              <div class="truncate text-sm font-semibold">{playlist().name}</div>
                              <div class="mt-1 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">
                                playlist · {(playlist().tracks || []).length} items
                              </div>
                            </div>
                          )}
                        </Show>
                        <Show when={!visiblePlaylistDetail() && (movieFilter() || artistFilter() || musicDirectorFilter())}>
                          <>
                            <Show when={movieFilter()}>
                              <div class="flex items-center gap-3">
                              <div class="truncate text-sm font-semibold">
                                {albumFilterMeta()?.year ? `${albumFilterMeta()?.album || "Album"} (${albumFilterMeta()?.year})` : (albumFilterMeta()?.album || "Album")}
                              </div>
                                <Show when={user()}>
                                  <button
                                    type="button"
                                    onClick={() => void toggleAlbumFavorite(albumFilterMeta() || { albumName: albumFilterMeta()?.album || "", albumUrl: "", year: albumFilterMeta()?.year || "" })}
                                    class={`shrink-0 transition-colors ${favoriteAlbumSet().has(movieFilter()) ? "text-[var(--fg)]" : "text-[var(--muted)] hover:text-[var(--fg)]"}`}
                                    aria-label={favoriteAlbumSet().has(movieFilter()) ? "Remove album from favorites" : "Add album to favorites"}
                                  >
                                    <HeartIcon filled={favoriteAlbumSet().has(movieFilter())} />
                                  </button>
                                </Show>
                              </div>
                            </Show>
                            <Show when={!movieFilter() && musicDirectorFilter()}>
                              <div class="flex items-center gap-3">
                                <div class="truncate text-sm font-semibold">{musicDirectorFilter()}</div>
                                <Show when={user()}>
                                  <button
                                    type="button"
                                    onClick={() => void toggleMusicDirectorFavorite(musicDirectorFilter())}
                                    class={`shrink-0 transition-colors ${favoriteMusicDirectorSet().has(musicDirectorFilter()) ? "text-[var(--fg)]" : "text-[var(--muted)] hover:text-[var(--fg)]"}`}
                                    aria-label={favoriteMusicDirectorSet().has(musicDirectorFilter()) ? "Remove music director from favorites" : "Add music director to favorites"}
                                  >
                                    <HeartIcon filled={favoriteMusicDirectorSet().has(musicDirectorFilter())} />
                                  </button>
                                </Show>
                              </div>
                            </Show>
                            <Show when={!movieFilter() && !musicDirectorFilter() && artistFilter()}>
                              <div class="truncate text-sm font-semibold">{artistFilter()}</div>
                            </Show>
                            <div class="mt-1 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">
                              {musicDirectorFilter() && !movieFilter() ? "movies" : "songs"} · {musicDirectorFilter() && !movieFilter() ? visibleAlbums().length : visibleResults().length} results
                            </div>
                          </>
                        </Show>
                        <Show when={!visiblePlaylistDetail() && !movieFilter() && !artistFilter() && !musicDirectorFilter()}>
                          <div class="hidden truncate text-sm font-semibold uppercase tracking-widest text-[var(--faint)] xl:block">
                            {searchTab() === "albums" ? `Albums · ${visibleAlbums().length}` : searchTab() === "music-directors" ? `Music Directors · ${visibleMusicDirectors().length}` : `Songs · ${visibleResults().length}`}
                          </div>
                        </Show>
                      </div>

                      <div class="flex w-full shrink-0 items-center justify-between gap-4 sm:w-auto sm:justify-start">
                        <Show when={visiblePlaylistDetail() && canManageVisiblePlaylist()}>
                          {(p) => (
                            <div class="flex items-center gap-3 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)]">
                              <button
                                type="button"
                                onClick={() => void clearVisiblePlaylist()}
                                disabled={playlistMutationBusy() === `clear:${p().id}`}
                                class="hover:text-[var(--fg)] disabled:text-[var(--line)]"
                              >
                                Clear
                              </button>
                              <button
                                type="button"
                                onClick={() => void deleteVisiblePlaylist()}
                                disabled={playlistMutationBusy() === `delete:${p().id}`}
                                class="hover:text-[var(--fg)] disabled:text-[var(--line)]"
                              >
                                Delete
                              </button>
                            </div>
                          )}
                        </Show>
                        <Show when={!visiblePlaylistDetail() && (movieFilter() || artistFilter() || musicDirectorFilter()) && libraryNavStack().length === 0}>
                          <button
                            type="button"
                            onClick={() => {
                              setMovieFilter("");
                              setAlbumFilterMeta(null);
                              setArtistFilter("");
                              setMusicDirectorFilter("");
                            }}
                            class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] transition hover:text-[var(--fg)]"
                          >
                            Reset view
                          </button>
                        </Show>
                      </div>
                    </header>

                    <Show when={mainTab() === "library" && visiblePlaylistDetail()}>
                      <div class="mobile-section-pad border-b border-[var(--line-soft)] xl:hidden">
                        <Show when={visiblePlaylistDetail()}>
                          {(playlist) => (
                            <div class="mobile-card flex items-start justify-between gap-3">
                              <div class="min-w-0">
                                <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Playlist</div>
                                <div class="mt-2 truncate text-lg font-semibold">{playlist().name}</div>
                                <div class="mt-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]">
                                  {(playlist().tracks || []).length} tracks
                                </div>
                              </div>
                              <button
                                type="button"
                                onClick={() => {
                                  setMainBrowseTab("playlists");
                                  setShowMobilePlaylistPicker(true);
                                }}
                                class="shrink-0 rounded-full border border-[var(--line)] px-4 py-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]"
                              >
                                Playlists
                              </button>
                            </div>
                          )}
                        </Show>
                      </div>
                    </Show>

                    <Show when={!visiblePlaylistDetail() && mainTab() === "library" && !movieFilter() && !artistFilter() && !musicDirectorFilter()}>
                      <div class="mobile-tab-row font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] sm:gap-4 sm:px-6">
                        <button
                          type="button"
                          onClick={() => setSearchTab("songs")}
                          class={`rounded-full border px-3 py-2 transition ${searchTab() === "songs" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}
                        >
                          Songs
                        </button>
                        <button
                          type="button"
                          onClick={() => setSearchTab("albums")}
                          class={`rounded-full border px-3 py-2 transition ${searchTab() === "albums" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}
                        >
                          Albums
                        </button>
                        <button
                          type="button"
                          onClick={() => setSearchTab("music-directors")}
                          class={`rounded-full border px-3 py-2 transition ${searchTab() === "music-directors" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}
                        >
                          Music Directors
                        </button>
                      </div>
                    </Show>

                    <Show
                      when={searchTab() === "albums" || searchTab() === "music-directors" || (musicDirectorFilter() && !movieFilter())}
                      fallback={
                        <>
                          <div class="song-table-header hidden border-b border-[var(--line-soft)] px-6 py-2 sm:grid">
                            <SortableSongHeader columnKey="default" label="#" sortKey={currentSongSort().key} sortDirection={currentSongSort().direction} onSort={toggleSongSort} class="song-col-index text-right" />
                            <SortableSongHeader columnKey="track" label="Song Name" sortKey={currentSongSort().key} sortDirection={currentSongSort().direction} onSort={toggleSongSort} class="song-col-title" />
                            <SortableSongHeader columnKey="movie" label="Movie" sortKey={currentSongSort().key} sortDirection={currentSongSort().direction} onSort={toggleSongSort} class="song-col-movie hidden md:block" />
                            <SortableSongHeader columnKey="musicDirector" label="Music Director" sortKey={currentSongSort().key} sortDirection={currentSongSort().direction} onSort={toggleSongSort} class="song-col-director hidden lg:block" />
                            <SortableSongHeader columnKey="singers" label="Singer" sortKey={currentSongSort().key} sortDirection={currentSongSort().direction} onSort={toggleSongSort} class="song-col-singers hidden xl:block" />
                            <SortableSongHeader columnKey="year" label="Year" sortKey={currentSongSort().key} sortDirection={currentSongSort().direction} onSort={toggleSongSort} class="song-col-year" />
                            <span class="song-col-action text-right font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Actions</span>
                          </div>
                          <Show when={!loading()} fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--muted)]">Loading{loadingDots()}</div>}>
                            <Show when={!error()} fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--soft)]">{error()}</div>}>
                              <Show
                                when={sortedActiveSongList().length > 0}
                                fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--muted)]">No results</div>}
                              >
                                <ul ref={listRef} class="min-h-0 flex-1 overflow-y-auto px-2">
                                  <For each={sortedActiveSongList()}>
                                    {(song, index) => {
                                      const active = () => selectedSong()?.id === song.id;
                                      return (
                                        <li>
                                  <button
                                    ref={(el) => {
                                      if (el) rowRefs.set(song.id, el);
                                      else rowRefs.delete(song.id);
                                    }}
                                    type="button"
                                    onClick={() => loadSong(song, true)}
                                    class={`song-table-row flex w-full flex-wrap items-start gap-x-3 gap-y-2 px-4 py-3 text-left transition ${
                                      active()
                                        ? currentTrackId() === song.id
                                          ? "song-row-active text-[var(--fg)]"
                                                  : "bg-[var(--hover)] text-[var(--fg)]"
                                                : "bg-transparent text-[var(--fg)] hover:bg-[var(--hover)]"
                                            }`}
                                          >
                                    <span class="song-col-index w-8 shrink-0 pt-0.5 text-right font-mono text-xs text-[var(--soft)] sm:pt-0">
                                      {currentTrackId() === song.id && isPlaying() && streamStarted() ? <PlayingBars /> : String(index() + 1).padStart(2, "0")}
                                    </span>
                                    <div class="song-col-title min-w-0 flex-1 basis-[calc(100%-5.75rem)] sm:basis-auto">
                                      <div class="truncate text-sm">{song.track}</div>
                                      <div class="mt-1 flex flex-wrap gap-x-2 gap-y-1 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--soft)] sm:hidden">
                                        <Show when={song.movie}><span>{song.movie}</span></Show>
                                        <Show when={song.musicDirector}><span>{song.musicDirector}</span></Show>
                                        <Show when={song.singers}><span>{song.singers}</span></Show>
                                        <Show when={song.year}><span>{song.year}</span></Show>
                                      </div>
                                    </div>
                                    <DrilldownText value={song.movie} payload={{ album: song.movie, albumUrl: song.albumUrl, year: song.year }} onClick={navigateToMovie} class="song-col-movie hidden font-mono text-[11px] text-[var(--soft)] md:block" />
                                    <DrilldownText value={song.musicDirector} onClick={navigateToMusicDirector} class="song-col-director hidden font-mono text-[11px] text-[var(--soft)] lg:block" />
                                    <span class="song-col-singers hidden truncate font-mono text-[11px] text-[var(--soft)] xl:block">{song.singers || "-"}</span>
                                    <span class="song-col-year hidden font-mono text-[11px] text-[var(--soft)] sm:block">{song.year || "-"}</span>
                                    <SongRowActions
                                      queued={queuedSongIds().has(song.id)}
                                      showFavorite={user()}
                                      favorite={favoriteIdSet().has(song.id)}
                                      onQueue={() => addSongToQueue(song)}
                                      onFavorite={() => void toggleFavorite(song.id)}
                                    />
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
                      }
                    >
                      <Show
                        when={searchTab() === "music-directors" && !musicDirectorFilter()}
                        fallback={
                          <>
                            <div class="hidden items-center gap-4 border-b border-[var(--line-soft)] px-6 py-2 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] sm:flex">
                              <span class="min-w-0 flex-1">Movie / Album Name</span>
                              <span class="w-20">Year</span>
                              <span class="w-24 text-right">Tracks</span>
                              <Show when={user()}>
                                <span class="w-8 text-right">Fav</span>
                              </Show>
                            </div>
                            <ul class="min-h-0 flex-1 overflow-y-auto px-2">
                              <For each={visibleAlbums()}>
                                {(album) => (
                                  <li>
                                    <button
                                      type="button"
                                      onClick={() => navigateToMovie(album)}
                                    class="mobile-list-row flex w-full flex-wrap items-start gap-x-3 gap-y-2 text-left transition hover:bg-[var(--hover)] sm:items-center sm:gap-4"
                                  >
                                      <div class="min-w-0 flex-1 basis-full sm:basis-auto">
                                        <div class="truncate text-sm">{album.album}</div>
                                        <div class="mt-1 flex flex-wrap gap-2 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--soft)] sm:hidden">
                                          <span>{album.year || "-"}</span>
                                          <span>{album.count ? `${album.count} songs` : ""}</span>
                                        </div>
                                      </div>
                                      <span class="hidden w-20 font-mono text-[11px] text-[var(--soft)] sm:block">{album.year || "-"}</span>
                                      <span class="hidden w-24 text-right font-mono text-[11px] text-[var(--soft)] sm:block">
                                        {album.count ? `${album.count} songs` : ""}
                                      </span>
                                      <Show when={user()}>
                                        <span class="flex w-8 justify-end">
                                          <span
                                            role="button"
                                            tabindex="-1"
                                            onClick={(event) => {
                                              event.stopPropagation();
                                            void toggleAlbumFavorite(album);
                                          }}
                                            class={`transition-colors ${favoriteAlbumSet().has(album.albumKey) ? "text-[var(--fg)]" : "text-[var(--muted)] hover:text-[var(--fg)]"}`}
                                          >
                                            <HeartIcon filled={favoriteAlbumSet().has(album.albumKey)} />
                                          </span>
                                        </span>
                                      </Show>
                                    </button>
                                  </li>
                                )}
                              </For>
                            </ul>
                          </>
                        }
                      >
                        <div class="hidden items-center gap-4 border-b border-[var(--line-soft)] px-6 py-2 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] sm:flex">
                          <span class="min-w-0 flex-1">Music Director</span>
                          <span class="w-20">Latest</span>
                          <span class="w-24 text-right">Songs</span>
                          <Show when={user()}>
                            <span class="w-8 text-right">Fav</span>
                          </Show>
                        </div>
                        <ul class="min-h-0 flex-1 overflow-y-auto px-2">
                          <For each={visibleMusicDirectors()}>
                            {(director) => (
                              <li>
                                <button
                                  type="button"
                                  onClick={() => navigateToMusicDirector(director.musicDirector)}
                                  class="mobile-list-row flex w-full items-center gap-4 text-left transition hover:bg-[var(--hover)]"
                                >
                                  <span class="min-w-0 flex-1 truncate text-sm">{director.musicDirector}</span>
                                  <span class="w-20 font-mono text-[11px] text-[var(--soft)]">{director.latestYear || "-"}</span>
                                  <span class="w-24 text-right font-mono text-[11px] text-[var(--soft)]">{director.count} songs</span>
                                  <Show when={user()}>
                                    <span class="flex w-8 justify-end">
                                      <span
                                        role="button"
                                        tabindex="-1"
                                        onClick={(event) => {
                                          event.stopPropagation();
                                          void toggleMusicDirectorFavorite(director.musicDirector);
                                        }}
                                        class={`transition-colors ${favoriteMusicDirectorSet().has(director.musicDirector) ? "text-[var(--fg)]" : "text-[var(--muted)] hover:text-[var(--fg)]"}`}
                                      >
                                        <HeartIcon filled={favoriteMusicDirectorSet().has(director.musicDirector)} />
                                      </span>
                                    </span>
                                  </Show>
                                </button>
                              </li>
                            )}
                          </For>
                        </ul>
                      </Show>
                    </Show>
                  </div>
                </Show>
              </div>
            </div>
          </section>
        </Show>

        <Show when={mainTab() !== "library" && (mainTab() !== "radio" && mainTab() !== "admin" && mainTab() !== "playlists")}>
          <section class="min-h-0 flex-1 overflow-hidden px-4 py-3 sm:px-6 sm:py-4">
            <div class="flex h-full min-h-0 flex-col overflow-hidden border border-[var(--line)] bg-[var(--panel)]">
              <Show when={mainTab() === "favorites"}>
                <div class="mobile-section-pad border-b border-[var(--line-soft)] sm:px-6">
                  <div class="mobile-card sm:hidden">
                    <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Favorites</div>
                    <div class="mt-2 text-lg font-semibold">Your pinned songs, albums, and composers.</div>
                  </div>
                  <div class="mt-3 flex gap-2 overflow-x-auto font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--soft)] sm:mt-0">
                    <button
                      type="button"
                      onClick={() => setFavoritesTab("songs")}
                      class={`shrink-0 rounded-full border px-3 py-2 transition ${favoritesTab() === "songs" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}
                    >
                      Songs
                    </button>
                    <button
                      type="button"
                      onClick={() => setFavoritesTab("albums")}
                      class={`shrink-0 rounded-full border px-3 py-2 transition ${favoritesTab() === "albums" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}
                    >
                      Albums
                    </button>
                    <button
                      type="button"
                      onClick={() => setFavoritesTab("music-directors")}
                      class={`shrink-0 rounded-full border px-3 py-2 transition ${favoritesTab() === "music-directors" ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]" : "border-[var(--line)] hover:border-[var(--fg)] hover:text-[var(--fg)]"}`}
                    >
                      Music Directors
                    </button>
                  </div>
                </div>
              </Show>
              <Show when={mainTab() === "queue"}>
                <div class="mobile-section-pad border-b border-[var(--line-soft)] sm:px-6">
                  <div class="mobile-card flex items-start justify-between gap-4 sm:border-0 sm:bg-transparent sm:p-0">
                    <div class="min-w-0">
                      <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Playback queue</div>
                      <div class="mt-2 text-lg font-semibold">Up next</div>
                      <div class="mt-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)]">
                        {queuedSongs().length} {queuedSongs().length === 1 ? "song" : "songs"}
                      </div>
                    </div>
                    <button
                      type="button"
                      onClick={clearPlaybackQueue}
                      disabled={!queuedSongs().length}
                      class="shrink-0 rounded-full border border-[var(--line)] px-4 py-2 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)] disabled:opacity-40"
                    >
                      Clear
                    </button>
                  </div>
                </div>
                <Show
                  when={queuedSongs().length > 0}
                  fallback={
                    <div class="flex flex-1 items-center justify-center px-6 text-center">
                      <div class="max-w-sm">
                        <div class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Queue empty</div>
                        <div class="mt-3 text-sm text-[var(--soft)]">Add songs from Library, Favorites, or Playlists. The next button will play queued songs first.</div>
                      </div>
                    </div>
                  }
                >
                  <ul class="min-h-0 flex-1 overflow-y-auto px-2 py-2 sm:px-0 sm:py-0">
                    <For each={queuedSongs()}>
                      {(song, index) => {
                        const active = () => currentTrackId() === song.id;
                        return (
                          <li>
                            <div class={`mobile-list-row flex items-center gap-3 transition ${active() ? "song-row-active text-[var(--fg)]" : "text-[var(--fg)] hover:bg-[var(--hover)]"}`}>
                              <button
                                type="button"
                                onClick={() => playQueuedSong(song, { allowCrossfade: true })}
                                class="flex min-w-0 flex-1 items-center gap-3 text-left"
                              >
                                <span class="w-8 shrink-0 text-right font-mono text-xs text-[var(--soft)]">
                                  {active() && isPlaying() && streamStarted() ? <PlayingBars /> : String(index() + 1).padStart(2, "0")}
                                </span>
                                <span class="min-w-0 flex-1">
                                  <span class="block truncate text-sm font-semibold">{song.track}</span>
                                  <span class="mt-1 flex flex-wrap gap-x-2 gap-y-1 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--soft)]">
                                    <Show when={song.movie}><span class="max-w-[12rem] truncate">{song.movie}</span></Show>
                                    <Show when={song.singers}><span class="max-w-[12rem] truncate">{song.singers}</span></Show>
                                    <Show when={song.year}><span>{song.year}</span></Show>
                                  </span>
                                </span>
                              </button>
                              <div class="flex shrink-0 items-center gap-2">
                                <button
                                  type="button"
                                  onClick={() => removeSongFromQueue(song.queueEntryId || song.id)}
                                  aria-label={`Remove ${song.track} from queue`}
                                  class="flex h-9 w-9 items-center justify-center rounded-full border border-[var(--line)] font-mono text-sm text-[var(--soft)] transition hover:border-[var(--fg)] hover:text-[var(--fg)]"
                                >
                                  x
                                </button>
                              </div>
                            </div>
                          </li>
                        );
                      }}
                    </For>
                  </ul>
                </Show>
              </Show>
              <Show
                when={mainTab() !== "queue" && (mainTab() !== "favorites" || favoritesTab() === "songs")}
                fallback={mainTab() === "queue" ? null : (
                  <Show
                    when={favoritesTab() === "albums"}
                    fallback={
                      <>
                        <div class="hidden items-center gap-4 border-b border-[var(--line-soft)] px-6 py-2 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] sm:flex">
                          <span class="min-w-0 flex-1">Music Director</span>
                          <span class="w-20">Latest</span>
                          <span class="w-24 text-right">Songs</span>
                          <Show when={user()}>
                            <span class="w-8 text-right">Fav</span>
                          </Show>
                        </div>
                        <Show
                          when={favoriteMusicDirectorRows().length > 0}
                          fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--muted)]">No favorite music directors</div>}
                        >
                          <ul class="min-h-0 flex-1 overflow-y-auto">
                            <For each={favoriteMusicDirectorRows()}>
                              {(director) => (
                                <li>
                                  <button
                                    type="button"
                                    onClick={() => navigateToMusicDirector(director.musicDirector)}
                                    class="mobile-list-row flex w-full items-center gap-4 text-left transition hover:bg-[var(--hover)]"
                                  >
                                    <span class="min-w-0 flex-1 truncate text-sm">{director.musicDirector}</span>
                                    <span class="w-20 font-mono text-[11px] text-[var(--soft)]">{director.latestYear || "-"}</span>
                                    <span class="w-24 text-right font-mono text-[11px] text-[var(--soft)]">{director.count} songs</span>
                                    <Show when={user()}>
                                      <span class="flex w-8 justify-end">
                                        <span
                                          role="button"
                                          tabindex="-1"
                                          onClick={(event) => {
                                            event.stopPropagation();
                                            void toggleMusicDirectorFavorite(director.musicDirector);
                                          }}
                                          class={`transition-colors ${favoriteMusicDirectorSet().has(director.musicDirector) ? "text-[var(--fg)]" : "text-[var(--muted)] hover:text-[var(--fg)]"}`}
                                        >
                                          <HeartIcon filled={favoriteMusicDirectorSet().has(director.musicDirector)} />
                                        </span>
                                      </span>
                                    </Show>
                                  </button>
                                </li>
                              )}
                            </For>
                          </ul>
                        </Show>
                      </>
                    }
                  >
                    <div class="hidden items-center gap-4 border-b border-[var(--line-soft)] px-6 py-2 font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)] sm:flex">
                      <span class="min-w-0 flex-1">Album</span>
                      <span class="w-20">Year</span>
                      <span class="w-24 text-right">Songs</span>
                      <Show when={user()}>
                        <span class="w-8 text-right">Fav</span>
                      </Show>
                    </div>
                    <Show
                      when={favoriteAlbumRows().length > 0}
                      fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--muted)]">No favorite albums</div>}
                    >
                      <ul class="min-h-0 flex-1 overflow-y-auto">
                        <For each={favoriteAlbumRows()}>
                          {(album) => (
                            <li>
                              <button
                                type="button"
                                onClick={() => navigateToMovie(album)}
                                class="mobile-list-row flex w-full items-center gap-4 text-left transition hover:bg-[var(--hover)]"
                              >
                                <span class="min-w-0 flex-1 truncate text-sm">{album.album}</span>
                                <span class="w-20 font-mono text-[11px] text-[var(--soft)]">{album.year || "-"}</span>
                                <span class="w-24 text-right font-mono text-[11px] text-[var(--soft)]">{album.count} songs</span>
                                <Show when={user()}>
                                  <span class="flex w-8 justify-end">
                                    <span
                                      role="button"
                                      tabindex="-1"
                                      onClick={(event) => {
                                        event.stopPropagation();
                                        void toggleAlbumFavorite(album);
                                      }}
                                      class={`transition-colors ${favoriteAlbumSet().has(album.albumKey) ? "text-[var(--fg)]" : "text-[var(--muted)] hover:text-[var(--fg)]"}`}
                                    >
                                      <HeartIcon filled={favoriteAlbumSet().has(album.albumKey)} />
                                    </span>
                                  </span>
                                </Show>
                              </button>
                            </li>
                          )}
                        </For>
                      </ul>
                    </Show>
                  </Show>
                )}
              >
                <div class="song-table-header hidden border-b border-[var(--line-soft)] px-6 py-2 sm:grid">
                  <SortableSongHeader columnKey="default" label="#" sortKey={currentSongSort().key} sortDirection={currentSongSort().direction} onSort={toggleSongSort} class="song-col-index text-right" />
                  <SortableSongHeader columnKey="track" label="Song Name" sortKey={currentSongSort().key} sortDirection={currentSongSort().direction} onSort={toggleSongSort} class="song-col-title" />
                  <SortableSongHeader columnKey="movie" label="Movie" sortKey={currentSongSort().key} sortDirection={currentSongSort().direction} onSort={toggleSongSort} class="song-col-movie hidden md:block" />
                  <SortableSongHeader columnKey="musicDirector" label="Music Director" sortKey={currentSongSort().key} sortDirection={currentSongSort().direction} onSort={toggleSongSort} class="song-col-director hidden lg:block" />
                  <SortableSongHeader columnKey="singers" label="Singer" sortKey={currentSongSort().key} sortDirection={currentSongSort().direction} onSort={toggleSongSort} class="song-col-singers hidden xl:block" />
                  <SortableSongHeader columnKey="year" label="Year" sortKey={currentSongSort().key} sortDirection={currentSongSort().direction} onSort={toggleSongSort} class="song-col-year" />
                  <span class="song-col-action text-right font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--faint)]">Actions</span>
                </div>

                <Show when={!loading()} fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--muted)]">Loading{loadingDots()}</div>}>
                  <Show when={!error()} fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--soft)]">{error()}</div>}>
                    <Show
                      when={sortedActiveSongList().length > 0}
                      fallback={<div class="flex flex-1 items-center justify-center font-mono text-xs uppercase tracking-[0.25em] text-[var(--muted)]">{mainTab() === "favorites" ? "No favorite songs yet" : "No results"}</div>}
                    >
                      <ul ref={listRef} class="min-h-0 flex-1 overflow-y-auto">
                        <For each={sortedActiveSongList()}>
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
                                class={`song-table-row flex w-full flex-wrap items-start gap-x-3 gap-y-2 px-4 py-3 text-left transition sm:px-6 ${
                                  active()
                                    ? currentTrackId() === song.id
                                      ? "song-row-active text-[var(--fg)]"
                                        : "bg-[var(--hover)] text-[var(--fg)]"
                                      : "bg-transparent text-[var(--fg)] hover:bg-[var(--hover)]"
                                  }`}
                                >
                                  <span class="song-col-index w-8 shrink-0 pt-0.5 text-right font-mono text-xs text-[var(--soft)] sm:pt-0">
                                    {currentTrackId() === song.id && isPlaying() && streamStarted() ? <PlayingBars /> : String(index() + 1).padStart(2, "0")}
                                  </span>
                                  <div class="song-col-title min-w-0 flex-1 basis-[calc(100%-5.75rem)] sm:basis-auto">
                                    <div class="truncate text-sm">{song.track}</div>
                                    <div class="mt-1 flex flex-wrap gap-x-2 gap-y-1 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--soft)] sm:hidden">
                                      <Show when={song.movie}><span>{song.movie}</span></Show>
                                      <Show when={song.musicDirector}><span>{song.musicDirector}</span></Show>
                                      <Show when={song.singers}><span>{song.singers}</span></Show>
                                      <Show when={song.year}><span>{song.year}</span></Show>
                                    </div>
                                  </div>
                                  <DrilldownText value={song.movie} payload={{ album: song.movie, albumUrl: song.albumUrl, year: song.year }} onClick={navigateToMovie} class="song-col-movie hidden font-mono text-[11px] text-[var(--soft)] md:block" />
                                  <DrilldownText value={song.musicDirector} onClick={navigateToMusicDirector} class="song-col-director hidden font-mono text-[11px] text-[var(--soft)] lg:block" />
                                  <span class="song-col-singers hidden truncate font-mono text-[11px] text-[var(--soft)] xl:block">
                                    {song.singers || "-"}
                                  </span>
                                  <span class="song-col-year hidden font-mono text-[11px] text-[var(--soft)] sm:block">
                                    {song.year || "-"}
                                  </span>
                                  <Show
                                    when={mainTab() === "favorites"}
                                    fallback={
                                      <SongRowActions
                                        queued={queuedSongIds().has(song.id)}
                                        showFavorite={user()}
                                        favorite={favoriteIdSet().has(song.id)}
                                        onQueue={() => addSongToQueue(song)}
                                        onFavorite={() => void toggleFavorite(song.id)}
                                      />
                                    }
                                  >
                                    <SongRowActions
                                      queued={queuedSongIds().has(song.id)}
                                      showFavorite={user()}
                                      favorite={favoriteIdSet().has(song.id)}
                                      onQueue={() => addSongToQueue(song)}
                                      onFavorite={() => void toggleFavorite(song.id)}
                                    />
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
              </Show>
            </div>
          </section>
        </Show>
      </section>

      <footer class="relative z-30 shrink-0 border-t border-[var(--line)] bg-[var(--bg)] px-3 py-3 pb-[calc(0.75rem+env(safe-area-inset-bottom))] sm:px-6">
        <Show when={currentTrackId()}>
        <div class="md:hidden">
          <div class="rounded-[22px] border border-[var(--line)] bg-[linear-gradient(180deg,rgba(255,255,255,0.08),rgba(255,255,255,0.03))] px-3 py-3 shadow-[0_-12px_40px_rgba(0,0,0,0.25)]">
            <div class="flex w-full items-center gap-3">
              <button
                type="button"
                onClick={() => setShowMobilePlayerPanel(true)}
                class="flex min-w-0 flex-1 items-center gap-3 text-left"
                aria-label="Open now playing controls"
              >
                <div class="flex h-12 w-12 shrink-0 items-center justify-center rounded-[16px] border border-[var(--line)] bg-[rgba(255,255,255,0.04)] font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--soft)]">
                  <Show when={currentSong() && isPlaying() && streamStarted()} fallback={<span class="h-2 w-2 rounded-full bg-[var(--soft)]" />}>
                    <PlayingBars />
                  </Show>
                </div>
                <div class="min-w-0 flex-1">
                  <Show when={currentSong()} fallback={<p class="font-mono text-[10px] uppercase tracking-[0.25em] text-[var(--muted)]">No track selected</p>}>
                    {(song) => (
                      <>
                        <p class="truncate text-sm font-semibold">{song().track}</p>
                        <div class="mt-1 truncate font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--soft)]">
                          <Show when={song().singers} fallback={<Show when={song().movie}><span>{song().movie}</span></Show>}>
                            <span>{song().singers}</span>
                          </Show>
                        </div>
                      </>
                    )}
                  </Show>
                </div>
              </button>
              <div class="flex shrink-0 items-center gap-2">
                <button
                  type="button"
                  onClick={() => void selectRelative(-1, true, currentTrackId() || selectedId(), { forceImmediate: true })}
                  disabled={radioPlaybackLocked()}
                  aria-label="Previous"
                  class="flex h-9 w-9 items-center justify-center rounded-full border border-[var(--line)] text-[var(--soft)] disabled:opacity-40"
                >
                  <PrevIcon />
                </button>
                <button
                  type="button"
                  onClick={togglePlayback}
                  aria-label={isPlaying() ? (streamStarted() ? "Pause" : "Loading") : "Play"}
                  class="flex h-10 w-10 items-center justify-center rounded-full border border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]"
                >
                  {isPlaying() ? (streamStarted() ? <PauseIcon /> : <LoadingSpinnerIcon />) : <PlayIcon />}
                </button>
                <button
                  type="button"
                  onClick={() => void playNextFromQueueOrRelative({ forceImmediate: true })}
                  disabled={radioPlaybackLocked()}
                  aria-label="Next"
                  class="flex h-9 w-9 items-center justify-center rounded-full border border-[var(--line)] text-[var(--soft)] disabled:opacity-40"
                >
                  <NextIcon />
                </button>
                <Show when={user() && currentSong()}>
                  <button
                    type="button"
                    onClick={() => void toggleFavorite(currentSong().id)}
                    aria-label={favoriteIdSet().has(currentSong()?.id) ? "Remove from favorites" : "Add to favorites"}
                    class={`flex h-9 w-9 items-center justify-center rounded-full border transition ${
                      favoriteIdSet().has(currentSong()?.id)
                        ? "border-[var(--fg)] text-[var(--fg)]"
                        : "border-[var(--line)] text-[var(--soft)]"
                    }`}
                  >
                    <HeartIcon filled={favoriteIdSet().has(currentSong()?.id)} />
                  </button>
                </Show>
                <button
                  type="button"
                  onClick={() => setShowMobilePlayerPanel(true)}
                  aria-label="Expand player"
                  class="flex h-9 w-9 items-center justify-center rounded-full border border-[var(--line)] text-[var(--soft)]"
                >
                  <ChevronUpIcon />
                </button>
              </div>
            </div>
          </div>
        </div>
        </Show>

        <div class="hidden md:block">
        <div class="mb-2 flex justify-center">
          <div class="min-w-0 max-w-2xl text-center">
            <Show when={currentSong()} fallback={<p class="font-mono text-xs uppercase tracking-[0.25em] text-[var(--muted)]">No track selected</p>}>
              {(song) => (
                <button
                  type="button"
                  onClick={() => openCurrentSongAlbum(song())}
                  class="truncate text-[13px] font-semibold transition hover:text-[var(--soft)]"
                >
                  {song().track}
                </button>
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
          <button
            type="button"
            onClick={() => setShowRemainingTime((prev) => !prev)}
            class="min-w-[2.5rem] text-left font-mono text-[10px] text-[var(--muted)] transition hover:text-[var(--fg)]"
          >
            {showRemainingTime() ? `-${formatTime(Math.max(0, duration() - currentTime()))}` : formatTime(duration())}
          </button>
        </div>

        <div class="grid grid-cols-[1fr_auto_1fr] items-center gap-4">
          <div class="flex items-center gap-2">
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
            <Show when={currentSong()}>
              <span class="group relative inline-flex">
                <button
                  type="button"
                  onClick={addCurrentSongToQueue}
                  aria-label={queuedSongIds().has(currentSong()?.id) ? "Already in queue" : "Add current song to queue"}
                  class={`flex h-8 items-center gap-2 rounded-full border px-3 transition ${
                    queuedSongIds().has(currentSong()?.id)
                      ? "border-[var(--fg)] text-[var(--fg)]"
                      : "border-[var(--line)] text-[var(--soft)] hover:border-[var(--fg)] hover:text-[var(--fg)]"
                  }`}
                >
                  <QueueIcon />
                  <span class="font-mono text-[10px] uppercase tracking-[0.18em]">Queue</span>
                </button>
                <TooltipBubble text={queuedSongIds().has(currentSong()?.id) ? "Already in queue" : "Add to queue"} position="bottom-full left-0 mb-2" />
              </span>
            </Show>
          </div>
          <div class="flex items-center justify-center gap-5">
            <IconButton disabled={radioPlaybackLocked()} onClick={() => selectRelative(-1, true, currentTrackId() || selectedId(), { forceImmediate: true })} label="Previous">
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
            <IconButton disabled={radioPlaybackLocked()} onClick={() => playNextFromQueueOrRelative({ forceImmediate: true })} label="Next">
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
        </div>
        <Show when={showMobilePlayerPanel() && currentTrackId()}>
          <div class="fixed inset-0 z-50 flex items-end bg-black/55 md:hidden">
            <button
              type="button"
              aria-label="Close player"
              class="absolute inset-0"
              onClick={() => setShowMobilePlayerPanel(false)}
            />
            <div
              class="relative z-10 w-full rounded-t-[32px] border-t border-[var(--line)] bg-[var(--bg)] px-5 pb-[calc(1.25rem+env(safe-area-inset-bottom))] pt-5 shadow-2xl transition-transform duration-150"
              style={{ transform: `translateY(${mobilePlayerDragOffset()}px)` }}
              onTouchStart={(event) => {
                mobilePlayerTouchStartY = event.touches[0]?.clientY ?? null;
              }}
              onTouchMove={(event) => {
                if (mobilePlayerTouchStartY == null) {
                  return;
                }
                const delta = (event.touches[0]?.clientY ?? mobilePlayerTouchStartY) - mobilePlayerTouchStartY;
                setMobilePlayerDragOffset(Math.max(0, delta));
              }}
              onTouchEnd={() => {
                if (mobilePlayerDragOffset() > 90) {
                  setShowMobilePlayerPanel(false);
                }
                setMobilePlayerDragOffset(0);
                mobilePlayerTouchStartY = null;
              }}
            >
              <div class="mx-auto h-1.5 w-14 rounded-full bg-[var(--line)]" />
              <div class="mt-5 min-w-0">
                <div class="font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--faint)]">Now playing</div>
                <div class="min-w-0">
                  <Show when={currentSong()} fallback={<div class="mt-2 text-base font-semibold">No track selected</div>}>
                    {(song) => (
                      <>
                        <button
                          type="button"
                          onClick={() => {
                            openCurrentSongAlbum(song());
                          }}
                          class="mt-2 block max-w-full truncate text-left text-2xl font-semibold transition hover:text-[var(--soft)]"
                        >
                          {song().track}
                        </button>
                        <div class="mt-2 flex flex-wrap items-center gap-x-2 gap-y-1 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--soft)]">
                          <Show when={song().movie}>
                            <button
                              type="button"
                              onClick={() => {
                                openCurrentSongAlbum(song());
                              }}
                              class="transition hover:text-[var(--fg)]"
                            >
                              {song().movie}
                            </button>
                          </Show>
                          <Show when={song().singers}><span>{song().singers}</span></Show>
                          <Show when={song().musicDirector}><span>{song().musicDirector}</span></Show>
                        </div>
                      </>
                    )}
                  </Show>
                </div>
              </div>

              <div class="mt-6 flex items-center gap-3">
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
                <button
                  type="button"
                  onClick={() => setShowRemainingTime((prev) => !prev)}
                  class="w-12 text-right font-mono text-[10px] text-[var(--muted)]"
                >
                  {showRemainingTime() ? `-${formatTime(Math.max(0, duration() - currentTime()))}` : formatTime(duration())}
                </button>
              </div>

              <div class="mt-6 flex items-center justify-center gap-4">
                <button
                  type="button"
                  onClick={() => selectRelative(-1, true, currentTrackId() || selectedId(), { forceImmediate: true })}
                  disabled={radioPlaybackLocked()}
                  aria-label="Previous"
                  class="flex h-12 w-12 items-center justify-center rounded-full border border-[var(--line)] text-[var(--soft)] disabled:opacity-40"
                >
                  <PrevIcon />
                </button>
                <button
                  type="button"
                  onClick={togglePlayback}
                  aria-label={isPlaying() ? (streamStarted() ? "Pause" : "Loading") : "Play"}
                  class="flex h-16 w-16 items-center justify-center rounded-full border border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]"
                >
                  {isPlaying() ? (streamStarted() ? <PauseIcon /> : <LoadingSpinnerIcon />) : <PlayIcon />}
                </button>
                <button
                  type="button"
                  onClick={() => playNextFromQueueOrRelative({ forceImmediate: true })}
                  disabled={radioPlaybackLocked()}
                  aria-label="Next"
                  class="flex h-12 w-12 items-center justify-center rounded-full border border-[var(--line)] text-[var(--soft)] disabled:opacity-40"
                >
                  <NextIcon />
                </button>
              </div>

              <div class="mt-6 flex items-center justify-center gap-2">
                <Show when={user() && currentSong()}>
                  <button
                    type="button"
                    onClick={() => {
                      void toggleFavorite(currentSong().id);
                    }}
                    aria-label={favoriteIdSet().has(currentSong()?.id) ? "Remove favorite" : "Add favorite"}
                    class={`inline-flex h-9 w-9 items-center justify-center rounded-full border ${
                      favoriteIdSet().has(currentSong()?.id)
                        ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]"
                        : "border-[var(--line)] text-[var(--soft)]"
                    }`}
                  >
                    <HeartIcon filled={favoriteIdSet().has(currentSong()?.id)} />
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      void saveCurrentToPlaylist();
                    }}
                    aria-label="Add to playlist"
                    class="inline-flex h-9 w-9 items-center justify-center rounded-full border border-[var(--line)] text-[var(--soft)]"
                  >
                    <PlusIcon />
                  </button>
                </Show>
                <Show when={currentSong()}>
                  <button
                    type="button"
                    onClick={() => {
                      addCurrentSongToQueue();
                    }}
                    aria-label={queuedSongIds().has(currentSong()?.id) ? "Already in queue" : "Add to queue"}
                    class={`inline-flex h-9 w-9 items-center justify-center rounded-full border ${
                      queuedSongIds().has(currentSong()?.id)
                        ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]"
                        : "border-[var(--line)] text-[var(--soft)]"
                    }`}
                  >
                    <QueueIcon />
                  </button>
                </Show>
                <button
                  type="button"
                  onClick={() => {
                    cyclePlaybackMode();
                  }}
                  disabled={radioPlaybackLocked()}
                  aria-label={`Playback mode: ${playbackModeLabel()}`}
                  class={`inline-flex h-9 w-9 items-center justify-center rounded-full border ${
                    repeatMode() !== "off" && !radioPlaybackLocked()
                      ? "border-[var(--fg)] bg-[var(--fg)] text-[var(--bg)]"
                      : "border-[var(--line)] text-[var(--soft)]"
                  } disabled:opacity-40`}
                >
                  {playbackModeIcon()}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    cyclePlaybackSpeed();
                  }}
                  aria-label={`Playback speed: ${formatPlaybackSpeed(playbackSpeed())}`}
                  class="inline-flex h-9 min-w-[2.8rem] items-center justify-center rounded-full border border-[var(--line)] px-2 font-mono text-[11px] text-[var(--soft)]"
                >
                  <span>{formatPlaybackSpeed(playbackSpeed())}</span>
                </button>
              </div>
            </div>
          </div>
        </Show>
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
                const queued = takeNextQueuedSong();
                if (queued?.song) {
                  loadSong(queued.song, true, {
                    allowCrossfade: true,
                    playbackContextSource: "queue",
                    playbackContextIds: queuePlaybackContextIds(queued.contextIds),
                  });
                  return;
                }
                if (repeatMode() === "album" || repeatMode() === "random") {
                  selectRelative(1, true, currentTrackId() || selectedId(), { allowCrossfade: true });
                  return;
                }
                const activeContextSongs = playbackContextSongs().length ? playbackContextSongs() : sortedActiveSongList();
                const current = activeContextSongs.findIndex((song) => song.id === (currentTrackId() || selectedId()));
                if (current >= 0 && current < activeContextSongs.length - 1) {
                  selectRelative(1, true, currentTrackId() || selectedId(), { allowCrossfade: true });
                } else {
                  setIsPlaying(false);
                }
              }}
              onError={(event) => {
                if (event.currentTarget !== getActiveAudio() || event.currentTarget.dataset.songId !== currentTrackId()) {
                  return;
                }
                setIsPlaying(false);
                setStreamStarted(false);
                if (autoplayNext()) {
                  setTimeout(() => {
                    if (currentTrackId() === event.currentTarget.dataset.songId) {
                      playNextFromQueueOrRelative({ allowCrossfade: true });
                    }
                  }, 1500);
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
                onInput={(event) => setPlaylistNameInput(sanitizePlaylistName(event.currentTarget.value))}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    event.preventDefault();
                    void createPlaylist();
                  }
                }}
                placeholder="Playlist name"
                maxLength={PLAYLIST_NAME_MAX_LENGTH}
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
                  disabled={playlistCreateBusy()}
                  class={`border px-4 py-2 font-mono text-[10px] uppercase tracking-[0.22em] transition ${
                    playlistCreateBusy() ? "cursor-not-allowed border-[var(--line-soft)] text-[var(--line)]" : "border-[var(--fg)] text-[var(--fg)] hover:bg-[var(--fg)] hover:text-[var(--bg)]"
                  }`}
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
