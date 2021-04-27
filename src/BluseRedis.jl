module BluseRedis

import JSON
import Redis
using Dates

export get_target, get_src_radec, get_antennas

"""
Cache hash keys for a given history key (e.g. `history:array_1:target`).
"""
const HKEY_CACHE = Dict{String, Array{String}}()

# TODO Get this from RadioInterferometry
"""
Convert String with `dd:mm:ss.s` format to Float64 degrees.
Also convert Real degrees to Float64 degrees.
"""
function dms2d(dms::AbstractString)::Float64
  d, m, s = map(x->parse(Float64,x), split(dms * ":0:0", ":"))
  sign = 1
  if '-' in dms
    sign = -1
    d = -d
  end
  d += m/60 + s/3600
  sign * d
end
dms2d(d::Real)::Float64 = Float64(d)
"""
Convert String with `hh:mm:ss.s` format to Float64 hours.
Also convert Real hours to Float64 hours.
"""
hms2h = dms2d

"""
Return all hash keys for history item specified by `histtype` and `subarray`.
The hash keys for a given history item are formatter as `yyyymmddTHHMMSS.sssZ`.
"""
function get_history_hkeys(redis::Redis.RedisConnection, histtype::String; subarray::String="array_1")
  rkey = "history:$subarray:$histtype"
  if haskey(HKEY_CACHE, rkey)
    HKEY_CACHE[subarray]
  else
    HKEY_CACHE[subarray] = Redis.hkeys(redis, rkey) |> sort
  end
end

"""
Return a pair of hash keys that bracket `dt` for the history item specified by
`histtype` and `subarray`.  If `dt` is before the first or after the last
history item, one of the returned keys will be `nothing`.
"""
function find_history_hkeys(redis::Redis.RedisConnection,
                            dt::DateTime,
                            histtype::String;
                            subarray::String="array_1")
  hkeys = get_history_hkeys(redis, histtype; subarray=subarray)
  dtstr = Dates.format(dt, "yyyymmddTHHMMSS.sssZ")

  lo = findlast(<=(dtstr), hkeys)
  hi = findfirst(>(dtstr), hkeys)

  (
   isnothing(lo) ? nothing : hkeys[lo],
   isnothing(hi) ? nothing : hkeys[hi]
  )
end

"""
Return the value of the history item specified by `histtype` and `subarray`
that is closest to `dt`.  Will warn if the closest history item is further than
`maxdist` away from `dt`.
"""
function get_history_item(redis::Redis.RedisConnection,
                          dt::DateTime,
                          histtype::String;
                          maxdist::TimePeriod=Day(1),
                          subarray::String="array_1")
  rkey = "history:$subarray:$histtype"

  lo, hi = find_history_hkeys(redis, dt, histtype; subarray=subarray)

  if isnothing(lo)
    hkey = hi
    histdt = DateTime(hi, dateformat"yyyymmddTHHMMSS.sssZ")
  elseif isnothing(hi)
    hkey = lo
    histdt = DateTime(lo, dateformat"yyyymmddTHHMMSS.sssZ")
  else
    lodt = DateTime(lo, dateformat"yyyymmddTHHMMSS.sssZ")
    hidt = DateTime(hi, dateformat"yyyymmddTHHMMSS.sssZ")
    hkey = (dt-lodt < hidt-dt) ? lo : hi
    histdt = (dt-lodt < hidt-dt) ? lodt : hidt
  end

  dist = abs(dt-histdt)
  if dist > maxdist
    @warn "$histtype item closest to $dt is $(canonicalize(dist)) away"
  end

  Redis.hget(redis, rkey, hkey)
end

"""
    get_target(redis::Redis.RedisConnection, dt::DateTime;
               maxdist::TimePeriod=Minute(1),
               subarray::String="array_1")

Return the target info for `subarray` from `redis` that has the closest
timestamp to `dt`.  A warning message will be printed the item's timestamp is
further than `maxdist` away from `dt`.
"""
function get_target(redis::Redis.RedisConnection, dt::DateTime;
                    maxdist::TimePeriod=Minute(1),
                    subarray::String="array_1")
  get_history_item(redis, dt, "target"; maxdist=maxdist, subarray=subarray)
end

"""
    get_src_radec(redis::Redis.RedisConnection, dt::DateTime;
                  maxdist::TimePeriod=Minute(1),
                  subarray::String="array_1")

Return `(src_name=src_name, ra=ra, decl=dec)` of the target info for `subarray`
from `redis` that has the closest timestamp to `dt`.  A warning message will be
printed the item's timestamp is further than `maxdist` away from `dt`.  `ra`
and `dec` will be in degrees. Returns `("unknown, 0.0, 0.0)` if not an RA/Dec
target.
"""
function get_src_radec(redis::Redis.RedisConnection, dt::DateTime;
                       maxdist::TimePeriod=Minute(1),
                       subarray::String="array_1")
  info = get_history_item(redis, dt, "target"; maxdist=maxdist, subarray=subarray)
  names, purpose, x, y, fluxmodel = split(info * ",,,,", r", *")
  src = split(names, r" *\| *")[1]
  occursin("radec", purpose) ?
      (src_name=src, ra=15*hms2h(x), decl=dms2d(y)) :
      (src_name="unknown", ra=0.0, decl=0.0)
end

"""
    get_antennas(redis::Redis.RedisConnection, dt::DateTime;
                 maxdist::TimePeriod=Hour(12),
                 subarray::String="array_1")

Return the antennas info for `subarray` from `redis` that has the closest
timestamp to `dt`.  A warning message will be printed the item's timestamp is
further than `maxdist` away from `dt`.
"""
function get_antennas(redis::Redis.RedisConnection, dt::DateTime;
                      maxdist::TimePeriod=Hour(12),
                      subarray::String="array_1")
  get_history_item(redis, dt, "antennas";
                   maxdist=maxdist, subarray=subarray) |> JSON.parse
end

end # module
