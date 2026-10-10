# What the TV actually does on the wire

Captured 2026-10-10 with `tools/aquos_probe.py` against a Sharp Aquos
(LC-series, 2011-2016 era) with wallpaper mode enabled. Everything the
integration assumes about the protocol should trace back to a line here.

## Session shape

- TCP port 10002. The TV sends **nothing** on connect: no banner, no
  `Login:` prompt (this TV has no login configured).
- Every `\r`-terminated line is treated as a command. A blank line gets its
  own `ERR\r`. So sending blank credentials produces two `ERR` replies,
  each in its own TCP chunk a few ms apart, *before* the real reply. This
  is what broke the old fixed-count reads.
- Replies are `OK\r`, `ERR\r`, or `<value>\r`, arriving 2-8 ms after the
  command. Set commands (`POWR0`, `POWR1`) take ~70 ms.
- The TV **never closes the connection** on its own; the client closes.
- The TV is not slow. Per-command latency in the old code was its own
  blind reads waiting on data that never came.

## Values seen (TV on, HDMI 2)

| Query | Reply | Meaning |
|---|---|---|
| `POWR?` | `1` | on |
| `VOLM?` | `18` | volume 0-60 |
| `MUTE?` | `2` | unmuted (1 = muted) |
| `IAVD?` | `2` | HDMI 2 |
| `AVMD?` | `2` | AV mode |
| `WIDE?` | `8` | aspect |
| `OPGD?` | `ERR` | backlight: unsupported on this model |
| `IFGS?` | `ERR` | signal strength: not on HDMI |
| `CHTR?` | `ERR` | channel: not on HDMI |
| `ACHN?` | `ERR` | audio selection: rejected on HDMI |

## Power states

Three states, not two:

| State | `POWR?` | Status queries | Screen |
|---|---|---|---|
| on | `1` | answer | picture |
| wallpaper | `0` | all `ERR` | art slideshow (3 h after off) |
| standby | *(not yet captured)* | | black |

Transitions observed:

- on + `POWR0` → wallpaper, reads `0` within ~3 s. `OK` reply.
- wallpaper + `POWR0` → **no-op**. `OK` reply, stays in wallpaper, keeps
  reading `0`. A second `POWR0` is also a no-op. Off commands cannot reach
  standby from wallpaper; only the 3 h timeout does.
- wallpaper + `POWR1` → on. `OK` reply. For ~15 s after the command the TV
  answers status queries (`VOLM?` = `18`) while **still reporting
  `POWR?` = `0`**. Power-on is confirmed on the wire only after that.
- wallpaper + remote power button → **on**, not standby.

Consequences for the integration:

- Wallpaper mode is indistinguishable from off on the wire and should be
  treated as off. No special handling.
- A redundant `media_player.turn_off` to a TV in wallpaper mode is
  harmless. It did not cause the 2026-10-09 overnight "on"; that night the
  TV was answering volume and input queries, which it only does when on.
- After `turn_on`, an immediate refresh reads `0`. The entity must hold
  "on" for a grace period rather than flip back to off for one poll.
