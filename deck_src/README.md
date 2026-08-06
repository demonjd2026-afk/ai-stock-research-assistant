# Capstone deck source

`capstone_deck.html` is the source for `../capstone_final.pdf` — 18 slides, rendered
straight to PDF by headless Chrome. Image paths point at `../screenshots/`, so the deck
regenerates from the repo with no other assets.

## Regenerate

```bash
cd deck_src
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --headless --disable-gpu --no-sandbox --allow-file-access-from-files \
  --no-pdf-header-footer \
  --print-to-pdf=../capstone_final.pdf \
  "file://$PWD/capstone_deck.html"
```

Page geometry is set by `@page { size: 13.333in 7.5in }` — standard 16:9, one `.slide`
per page. Edit the HTML and re-run; there is no other build step.

Screenshots are embedded at full resolution, so the PDF lands around 5 MB. To shrink it,
downscale copies of `../screenshots/*.png` (e.g. `sips -Z 1500`) and point the HTML at those.
