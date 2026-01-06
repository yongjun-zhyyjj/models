#!/usr/bin/env python

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

fig = plt.figure(figsize=(7, 7))
ax = fig.add_subplot(111, projection="3d")

# --- Range (xy-plane) ---
xx, yy = np.meshgrid(np.linspace(-2, 2, 10), np.linspace(-2, 2, 10))
zz = np.zeros_like(xx)
ax.plot_surface(xx, yy, zz, alpha=0.5, color='cyan', label="Range(T)")

# --- Null space (z-axis) ---
z = np.linspace(-2, 2, 20)
ax.plot(np.zeros_like(z), np.zeros_like(z), z, 'r', linewidth=3, label="Null(T)")

# --- Example vector decomposition ---
v = np.array([1, 2, 3])      # original vector in R³
Tv = np.array([1, 2, 0])     # projection onto range (plane)
nv = v - Tv                  # null space component

# Draw original vector
ax.quiver(0, 0, 0, *v, color='k', linewidth=2, label='Vector v')
# Draw range component
ax.quiver(0, 0, 0, *Tv, color='blue', linewidth=2, label='Range component')
# Draw null component starting from range component
ax.quiver(*Tv, *nv, color='red', linewidth=2, label='Null component')

# Labels and view
ax.set_xlabel('X')
ax.set_ylabel('Y')
ax.set_zlabel('Z')
ax.view_init(elev=20, azim=30)
ax.legend()

plt.show()

