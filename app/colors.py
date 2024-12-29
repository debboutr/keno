import matplotlib.pyplot as plt
import numpy as np
import matplotlib.colors as mcolors

# Get a list of available colormaps
# print(plt.colormaps())

# Create a colormap object
cmap = plt.cm.get_cmap('viridis')  # Replace 'viridis' with any colormap name
# Get colors from the colormap
colors = cmap(np.linspace(0, 1, 80))  # Get 10 evenly spaced colors
# translate to hex color
color_ramp = [mcolors.to_hex(color) for color in colors]
