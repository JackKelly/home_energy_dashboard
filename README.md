# home_energy_dashboard
Visualise my home energy data recorded by https://github.com/JackKelly/envoy_recorder

Edit with `uvx marimo edit --sandboxed plot_solar_pv.py`.

Export to blog with `uvx marimo export html-wasm plot_solar_pv.py --output ~/dev/JackKelly.github.io/solar`. And then `cd ~/dev/JackKelly.github.io/solar` and run `uvx python remove_underscores.py` to remove leading underscores from filenames (the underscores confuse GitHub pages and Jekyll, and don't actually get published). And then push to github.

See [my blog post for more info](https://jack-kelly.com/blog/2026-01-19-plot-solar-pv), and a link
to a visualisation my live data.
