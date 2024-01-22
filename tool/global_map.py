'''
Autor: Mijie Pang
Date: 2023-08-20 19:50:38
LastEditTime: 2023-11-27 17:52:05
Description: designed for a flexible map plot for globe
'''
import os
import warnings
import numpy as np
from datetime import datetime
import matplotlib as mpl
import matplotlib.pyplot as plt
# import matplotlib.ticker as mticker
from matplotlib.colors import ListedColormap, Normalize, BoundaryNorm, LogNorm
import cartopy.crs as ccrs
import cartopy.feature as cfeature
# from cartopy.io.shapereader import Reader
from cartopy.util import add_cyclic_point
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter

warnings.filterwarnings(action='ignore')


class my_globe():

    def __init__(self,
                 projection='default',
                 central_longitude=180,
                 figsize=[8, 5],
                 dpi=300,
                 title='Created on %s' %
                 (datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                 display_axis=True,
                 ocean_mask=False) -> None:

        print('Plot project initiated')
        self.start = datetime.now()

        fig = plt.figure(figsize=figsize, dpi=dpi)
        plt.rcParams['font.family'] = 'Times New Roman'
        plt.rcParams['mathtext.default'] = 'regular'
        plt.rcParams['axes.unicode_minus'] = False

        if projection == 'default':

            self.projection = ccrs.PlateCarree(
                central_longitude=central_longitude)

            # Label axes of a Plate Carree projection with a central longitude of 180:
            ax = fig.add_subplot(1, 1, 1, projection=self.projection)
            ax.set_global()
            ax.coastlines(linewidth=0.6)

            if central_longitude == 0:
                ax.set_xticks([-180, -120, -60, 0, 60, 120, 180],
                              crs=ccrs.PlateCarree())
            elif central_longitude == 180:
                ax.set_xticks([0, 60, 120, 180, 240, 300, 360],
                              crs=ccrs.PlateCarree())

            ax.set_yticks([-90, -60, -30, 0, 30, 60, 90],
                          crs=ccrs.PlateCarree())
            lon_formatter = LongitudeFormatter(zero_direction_label=True)
            lat_formatter = LatitudeFormatter()
            ax.xaxis.set_major_formatter(lon_formatter)
            ax.yaxis.set_major_formatter(lat_formatter)

        elif projection == 'robinson':

            self.projection = ccrs.Robinson(
                central_longitude=central_longitude)

            ax = fig.add_subplot(1, 1, 1, projection=self.projection)

            # make the map global rather than have it zoom in to
            # the extents of any plotted data
            ax.set_global()
            # ax.stock_img()
            ax.coastlines(linewidth=0.6)
            ax.gridlines(linestyle='-', linewidth=0.3)
            # ax.add_feature(cfeature.BORDERS, linestyle='-')  # add borders

        ### turn on or off the axis ###
        if not display_axis:
            plt.axis('off')

        ### set the title ###
        if not title == False:
            ax.set_title(title, {'size': 14, 'color': 'k'})

        ### mask the ocean layer ###
        if ocean_mask:
            ax.add_feature(cfeature.OCEAN,
                           facecolor='w',
                           linewidth=0,
                           zorder=100)

        self.ax = ax
        self.fig = fig
        self.central_longitude = central_longitude

    ### set the title ###
    def set_title(self, title: str) -> None:

        self.ax.set_title(title, {'size': 14, 'color': 'k'})

    ### get all the artists ###
    def get_artist(self, ) -> None:

        return self.fig.get_children()

    ### add the colorbar ###
    def add_colorbar(
            self,
            #  position=[],
            ticks_position='right',
            orientation='vertical',
            label=None,
            label_position='right',
            label_rotation=90,
            labelpad=10,
            colors=[
                '#F0F0F0',
                '#F0F096',
                '#FA9600',
                '#FA0064',
                '#9632FA',
                '#6496FA',
                '#1414FA',
                '#141414',
            ],
            bounds=[0, 20, 40, 60, 80, 100, 120, 140],
            cmap=None,
            extend='max',
            shrink=0.6,
            aspect=20,
            fraction=0.03,
            pad=0.01,
            norm='default',
            display=True,
            **kwargs) -> None:

        ### set the cmap and colorbar ###
        if cmap is not None:
            cmap = plt.get_cmap(cmap)
        else:
            cmap = ListedColormap(colors)

        if norm == 'default':
            boundaries = bounds
            norm = BoundaryNorm(bounds, cmap.N, extend=extend)

        elif norm == 'log':
            boundaries = bounds
            norm = LogNorm(vmin=np.min(bounds),
                           vmax=np.max(bounds),
                           extend=extend)

        elif norm == 'continous':
            boundaries = None
            norm = Normalize(vmin=np.min(bounds), vmax=np.max(bounds))

        if display:

            # position = self.fig.add_axes(position)
            cbar = plt.colorbar(
                mpl.cm.ScalarMappable(cmap=cmap, norm=norm),
                ax=self.ax,
                boundaries=boundaries,  # Adding values for extensions.
                extend=extend,  # {'neither', 'both', 'min', 'max'}
                orientation=orientation,
                ticks=bounds,
                # cax=position,
                shrink=shrink,
                pad=pad,
                aspect=aspect,  # change the width
                fraction=fraction,  # change the size fraction
                **kwargs)

            cbar.ax.yaxis.set_ticks_position(ticks_position)

            cbar.set_label(label, rotation=label_rotation, labelpad=labelpad)
            cbar.ax.yaxis.set_label_position(label_position)

            # print(cbar.ax.get_position())

        self.norm = norm
        self.cmap = cmap
        self.bounds = bounds

    ### plot scatter ###
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
                             transform=self.projection,
                             cmap=cmap,
                             norm=norm,
                             marker=marker,
                             alpha=alpha,
                             **kwargs)

        return sc.get_children()

    ### plot contour with line ###
    def contourf(self,
                 lon: np.ndarray,
                 lat: np.ndarray,
                 data: np.ndarray,
                 levels=10,
                 add_cyclic=True,
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
            levels=levels,
            transform=ccrs.PlateCarree(
                central_longitude=self.central_longitude),
            cmap=self.cmap,
            norm=self.norm,
            **kwargs)

    ### plot quivers ###
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
        self.quiver = self.ax.quiver(
            lon,
            lat,
            wind_u,
            wind_v,
            color=color,
            width=width,
            scale=scale,
            headwidth=headwidth,
            transform=ccrs.PlateCarree(
                central_longitude=self.central_longitude),
            **kwargs)

    ### add the key to the quivers ###
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

    ### save figure ###
    def save(self,
             name='Dafault',
             path='.',
             create_dir=False,
             **kwargs) -> None:

        if create_dir:
            if not os.path.exists(path):
                os.makedirs(path)

        if not name.endswith(('.jpg', '.png', '.tif', '.tiff', '.svg', '.svgz',
                              '.eps', '.pdf', '.pgf', '.ps', '.raw', '.rgba')):

            name = name + '.png'

        plt.savefig(os.path.join(path, name), bbox_inches='tight', **kwargs)

        print('Plot took %.2f s' %
              ((datetime.now() - self.start).total_seconds()))

    def close(self):

        plt.close()


if __name__ == '__main__':

    model_lon = np.linspace(0.125, 359.875, 1440)
    model_lat = np.linspace(90, -90, 721)
    data = np.random.uniform(940, 1060, [len(model_lat), len(model_lon)])

    plot = my_globe(ocean_mask=True)
    plot.add_colorbar(bounds=[940, 960, 980, 1000, 1020, 1040, 1060],
                      cmap='jet',
                      extend='both',
                      norm='continous',
                      label='hpa')
    # plot.contourf(model_lon,
    #               model_lat,
    #               data,
    #               levels=10,
    #               hide_line=True,
    #               alpha=0.8)
    plot.scatter(model_lon,
                 model_lat,
                 data,
                 size=0.1,
                 meshgrid=True,
                 edgecolor=None)

    plot.save('test.png', path='.')
    plot.close()

    pass
