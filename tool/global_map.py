'''
Autor: Mijie Pang
Date: 2023-08-20 19:50:38
LastEditTime: 2024-04-17 19:59:33
Description: designed for a flexible map plot for globe
'''
import logging
import warnings
import numpy as np
from time import time
from datetime import datetime
import matplotlib as mpl
import matplotlib.pyplot as plt
# import matplotlib.ticker as mticker
from matplotlib.colors import (ListedColormap, Normalize, BoundaryNorm,
                               LogNorm, LinearSegmentedColormap)
import cartopy.crs as ccrs
import cartopy.feature as cfeature
# from cartopy.io.shapereader import Reader
from cartopy.util import add_cyclic_point

warnings.filterwarnings(action='ignore')


class MyGlobe():

    def __init__(self,
                 projection=None,
                 central_longitude=180,
                 figsize=(8, 5),
                 dpi=300,
                 title=None,
                 colors=None,
                 bounds=None,
                 cmap=None,
                 norm_type=None,
                 extend='max',
                 axis=True,
                 gridline=True,
                 ocean_mask=False) -> None:

        logging.info('Plot project initiated')
        self.start = time()

        if colors is None:
            colors = [
                '#F0F0F0',
                '#F0F096',
                '#FA9600',
                '#FA0064',
                '#9632FA',
                '#6496FA',
                '#1414FA',
                '#141414',
            ]
        if bounds is None:
            bounds = [0, 20, 40, 60, 80, 100, 120, 140]

        fig = plt.figure(figsize=figsize, dpi=dpi)
        plt.rcParams['font.family'] = 'Times New Roman'
        plt.rcParams['mathtext.default'] = 'regular'
        plt.rcParams['axes.unicode_minus'] = False

        if projection is None:

            self.projection = ccrs.PlateCarree(
                central_longitude=central_longitude)

        elif projection == 'robinson':

            self.projection = ccrs.Robinson(
                central_longitude=central_longitude)

            # ax.gridlines(linestyle='-', linewidth=0.3)
            # ax.add_feature(cfeature.BORDERS, linestyle='-')  # add borders

        ax = fig.add_subplot(1, 1, 1, projection=self.projection)
        ax.set_global()
        ax.coastlines(linewidth=0.6)

        self.ax = ax
        self.central_longitude = central_longitude

        # turn on or off the axis
        if not axis:
            plt.axis('off')

        if gridline:
            self.add_gridlines()

        # set the title
        if not title is None:
            self.set_title(title)

        ### mask the ocean layer ###
        if ocean_mask:
            self.ax.add_feature(cfeature.OCEAN,
                                facecolor='w',
                                linewidth=0,
                                zorder=100)

        cmap = self.set_colormap(cmap, colors, norm_type)
        norm = self.set_norm(bounds, cmap, norm_type, extend)
        self.cmap = cmap
        self.norm = norm
        self.bounds = bounds
        self.extend = extend

    ### *----------------------------------------* ###
    ### *---   plot the base background map   ---* ###
    ### *----------------------------------------* ###

    ### set the title ###
    def set_title(self, title: str, **kwargs) -> None:

        self.ax.set_title(title, {'size': 14, 'color': 'k'}, **kwargs)

    ### set the grid line ###
    def add_gridlines(self, **kwargs) -> None:

        gl = self.ax.gridlines(draw_labels=True,
                               x_inline=False,
                               y_inline=False,
                               linewidth=0.1,
                               color='gray',
                               alpha=0.8,
                               linestyle='--',
                               **kwargs)
        gl.top_labels = False
        gl.right_labels = False

    @staticmethod
    def set_colormap(cmap: str, colors: list, cmap_type=None) -> object:

        if cmap is None:
            if cmap_type is None:
                cmap = ListedColormap(colors)
            elif cmap_type in ('continuous', 'c'):
                cmap = LinearSegmentedColormap.from_list('mymap', colors)
        else:
            cmap = plt.get_cmap(cmap)

        return cmap

    @staticmethod
    def set_norm(bounds: list,
                 cmap: list,
                 norm_type=None,
                 extend=None) -> object:

        if norm_type is None:
            return BoundaryNorm(bounds, cmap.N, extend=extend)
        elif norm_type in ('log', 'l'):
            return LogNorm(vmin=np.min(bounds),
                           vmax=np.max(bounds),
                           extend=extend)
        elif norm_type in ('continuous', 'c'):
            return Normalize(vmin=np.min(bounds), vmax=np.max(bounds))
        else:
            raise ValueError('Unrecognized norm type : %s' % (norm_type))

    ### *--- add the colorbar ---* ###
    def add_colorbar(
            self,
            cmap=None,
            norm=None,
            bounds=None,
            extend=None,
            #  position=[],
            ticks_position='right',
            orientation='vertical',
            label=None,
            label_position='right',
            label_rotation=90,
            labelpad=10,
            shrink=0.6,
            aspect=20,
            fraction=0.03,
            pad=0.01,
            **kwargs) -> None:

        # position = self.fig.add_axes(position)
        cbar = plt.colorbar(
            mpl.cm.ScalarMappable(cmap=cmap or self.cmap,
                                  norm=norm or self.norm),
            ax=self.ax,
            # boundaries=bounds or self.bounds,  # Adding values for extensions.
            extend=extend or self.extend,  # {'neither', 'both', 'min', 'max'}
            orientation=orientation,
            ticks=bounds or self.bounds,
            # cax=position,
            shrink=shrink,
            pad=pad,
            aspect=aspect,  # change the width
            fraction=fraction,  # change the size fraction
            label=label,
            **kwargs)

        cbar.ax.yaxis.set_ticks_position(ticks_position)

        cbar.set_label(label, rotation=label_rotation, labelpad=labelpad)
        cbar.ax.yaxis.set_label_position(label_position)

    ### *--- plot scatter ---* ###
    def scatter(self,
                lon: np.ndarray,
                lat: np.ndarray,
                value: np.ndarray,
                color=None,
                edgecolor='black',
                size=8,
                marker='.',
                alpha=1,
                linewidths=0.3,
                colors=None,
                bounds=None,
                meshgrid=False,
                missing_value=None,
                **kwargs) -> None:

        ### fix the longitude drift ###
        lon = lon - self.central_longitude

        if meshgrid:
            lon, lat = np.meshgrid(lon, lat)

        if color is not None:
            value = color

        if missing_value is not None:
            value[value == missing_value] = np.nan

        if colors is not None or bounds is not None:
            cmap = ListedColormap(colors)
            norm = BoundaryNorm(bounds, cmap.N)
        else:
            cmap = self.cmap
            norm = self.norm

        sc = self.ax.scatter(lon,
                             lat,
                             s=size,
                             c=value,
                             edgecolor=edgecolor,
                             linewidths=linewidths,
                             transform=ccrs.PlateCarree(
                                 central_longitude=self.central_longitude),
                             cmap=cmap,
                             norm=norm,
                             marker=marker,
                             alpha=alpha,
                             **kwargs)

        return sc.get_children()

    ### *--- plot contour with line ---* ###
    def contourf(self,
                 lon: np.ndarray,
                 lat: np.ndarray,
                 data: np.ndarray,
                 levels=None,
                 add_cyclic=False,
                 meshgrid=True,
                 hide_line=True,
                 **kwargs) -> None:

        ### fix the possible white line ###
        if add_cyclic:
            data, lon = add_cyclic_point(data, coord=lon)

        ### fix the longitude drift ###
        lon = lon - self.central_longitude

        ### meshgrid the gridlines ###
        if meshgrid:
            lon, lat = np.meshgrid(lon, lat)

        ### hide the contour line ###
        if hide_line:
            kwargs.update({'antialiased': True})

        ### contourf ###
        contourf_set = self.ax.contourf(
            lon,
            lat,
            data,
            levels=levels or self.bounds,
            transform=ccrs.PlateCarree(
                central_longitude=self.central_longitude),
            cmap=self.cmap,
            norm=self.norm,
            **kwargs)

    ### *--- plot quivers ---* ###
    def quiver(self,
               lon: np.ndarray,
               lat: np.ndarray,
               wind_u: np.ndarray,
               wind_v: np.ndarray,
               rescale=True,
               lon_fraction=10,
               lat_fraction=10,
               color='black',
               scale=600,
               width=0.001,
               headwidth=6,
               meshgrid=False,
               **kwargs) -> None:

        ### fix the longitude drift ###
        lon = lon - self.central_longitude

        ### decrease the data number ###
        if rescale:

            lon_residual = wind_u.shape[1] % lon_fraction
            lat_residual = wind_u.shape[0] % lat_fraction

            if not lon_residual == 0:
                wind_u = wind_u[:, :-lon_residual]
                wind_v = wind_v[:, :-lon_residual]
                lon = lon[:-lon_residual]

            if not lat_residual == 0:
                wind_u = wind_u[:-lat_residual, :]
                wind_v = wind_v[:-lat_residual, :]
                lat = lat[:-lat_residual]

            lon = np.mean(lon.reshape(len(lon) // lon_fraction, lon_fraction),
                          axis=1)
            lat = np.mean(lat.reshape(len(lat) // lat_fraction, lat_fraction),
                          axis=1)

            wind_u = np.mean(wind_u.reshape(wind_u.shape[0] // lat_fraction,
                                            lat_fraction,
                                            wind_u.shape[1] // lon_fraction,
                                            lon_fraction),
                             axis=(1, 3))
            wind_v = np.mean(wind_v.reshape(wind_v.shape[0] // lat_fraction,
                                            lat_fraction,
                                            wind_v.shape[1] // lon_fraction,
                                            lon_fraction),
                             axis=(1, 3))

        ### meshgrid the gridlines ###
        if meshgrid:

            lon, lat = np.meshgrid(lon, lat)

        ### plot the quiver ###
        self.quiver = self.ax.quiver(lon,
                                     lat,
                                     wind_u,
                                     wind_v,
                                     color=color,
                                     width=width,
                                     scale=scale,
                                     headwidth=headwidth,
                                     transform=self.projection,
                                     **kwargs)

    ### *--- add the key to the quivers ---* ###
    def quiverkey(self,
                  key_position=[0.95, 1.02],
                  length=10,
                  label='default',
                  labelsep=0.01,
                  labelpos='N',
                  font={},
                  **kwargs) -> None:

        default_font = {'size': 10, 'family': 'Times New Roman'}
        default_font.update(font)
        self.ax.quiverkey(self.quiver,
                          key_position[0],
                          key_position[1],
                          length,
                          label=label,
                          labelsep=labelsep,
                          labelpos=labelpos,
                          fontproperties=default_font,
                          **kwargs)

    ### *--- save figure ---* ###
    def save(self, name=None, **kwargs) -> None:

        name = datetime.now().strftime(
            '%Y%m%d_%H%M%S') if name is None else name

        plt.savefig(name, bbox_inches='tight', **kwargs)

        logging.info('Plot took %.2f s' % (time() - self.start))

    def close(self):

        plt.close()


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
    model_lon = np.linspace(0.125, 359.875, 144)
    model_lat = np.linspace(90, -90, 72)
    data1 = np.random.uniform(940, 1060, [len(model_lat), len(model_lon)])
    data2 = model_lon.reshape(1, -1) + model_lat.reshape(-1, 1) + 940

    plot = MyGlobe(
        bounds=[940, 960, 980, 1000, 1020, 1040, 1060],
        cmap='jet',
        extend='both',
        norm_type='c',
    )
    plot.add_colorbar()
    plot.contourf(model_lon, model_lat, data2, add_cyclic=False)
    plot.scatter(model_lon,
                 model_lat,
                 data1,
                 size=3,
                 meshgrid=True,
                 edgecolor=None)
    plot.save('test.png')
    plot.close()
