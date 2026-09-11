# Render uses the startCommand in render.yaml; this is the fallback if that ever goes
# missing. It must name the same thing. It used to start the signal bot alone, which meant
# losing render.yaml would quietly leave the copy bot -- the one that moves money -- not
# running at all.
worker: bash scripts/run_both.sh
