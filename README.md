# home_energy_dashboard
Visualise my home energy data recorded by https://github.com/JackKelly/envoy_recorder

Edit with `uvx marimo edit --sandbox plot_solar_pv.py`.

Upgrade dependencies in the Marimo web UI.

Export to blog with:

```bash
rm -rf ~/dev/JackKelly.github.io/solar
mkdir ~/dev/JackKelly.github.io/solar
uvx marimo export html-wasm plot_solar_pv.py --output ~/dev/JackKelly.github.io/solar
cd ~/dev/JackKelly.github.io/solar
uvx python ~/dev/python/home_energy_dashboard/remove_underscores.py

git commit -a -m "message"
git push
```

`remove_underscores.py` removes leading underscores from filenames because the underscores confuse GitHub pages and Jekyll, and don't actually get published.

See [my blog post for more info](https://jack-kelly.com/blog/2026-01-19-plot-solar-pv), and a link
to a visualisation my live data.
