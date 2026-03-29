# Purge nginx or openresty proxy_cache when slice is enabled

In my config we use slice to slit large files into small chunks into the
proxy_cache

```
slice 10m;
proxy_cache_bypass      $arg_nocache;
proxy_cache             google-cloud-storage;
proxy_cache_key         "${request_uri}$slice_range";
proxy_cache_valid       200 206 24h;
proxy_set_header        Range $slice_range;
expires 1h;

```

So I need a way to purge proxy_cache for the sliced content.

