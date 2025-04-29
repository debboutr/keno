import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colormaps

# Get a list of available colormaps
# print(plt.colormaps())

# Create a colormap object
cmap = colormaps["cividis_r"]  # Spectral
# Get colors from the colormap
colors = cmap(np.linspace(0, 1, 80)) * np.array([255, 255, 255, 0.5])
# translate to hex color
# color_ramp = [mcolors.to_hex(color) for color in colors][::-1]
color_ramp = [f"rgba{tuple(c.tolist())}" for c in colors]
