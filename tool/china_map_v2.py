'''
Autor: Mijie Pang
Date: 2024-02-20 22:07:44
LastEditTime: 2024-04-17 19:59:22
Description: 
'''
import logging
import warnings
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.colors import (ListedColormap, Normalize, BoundaryNorm,
                               LogNorm)
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.io.shapereader import Reader
from datetime import datetime

warnings.filterwarnings(action='ignore')
shp_dir = '/home/pangmj/package/shp_file/china'


class MyMap:

    def __init__(
        self,
        figsize=(8, 5),
        dpi=300,
        title=None,
        extent=None,
        colors=None,
        bounds=None,
        cmap=None,
        map2=False,
        colorbar=True,
        gridline=True,
        coastline=True,
        axis=True,
        colorbar_extend='max',
        colorbar_shrink=0.6,
        colorbar_aspect=20,
        colorbar_fraction=0.03,
        colorbar_pad=0.01,
        colorbar_norm=None,
    ):

        logging.info('Plot project initiated')
        self.start = datetime.now()

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
        if extent is None:
            extent = [80, 134.1, 15.5, 52.5]

        projection = ccrs.LambertConformal(central_longitude=107.5,
                                           central_latitude=36.0,
                                           standard_parallels=(25, 47))

        self.configure_plot_defaults()

        self.fig = plt.figure(figsize=figsize, dpi=dpi)
        self.ax = self.fig.add_subplot(1, 1, 1, projection=projection)
        self.set_extent(extent)
        self.set_title(title)

        if not axis:
            self.ax.axis('off')
        if coastline:
            self.add_coastline()

        self.add_china_border()

        if gridline:
            self.add_gridlines()

        cmap = self.set_colormap(cmap, colors)
        norm = self.set_norm(bounds, cmap, colorbar_norm, colorbar_extend)
        self.cmap = cmap
        self.norm = norm
        self.bounds = bounds
        self.extend = colorbar_extend

        if colorbar:
            self.add_colorbar(self.cmap,
                              self.norm,
                              self.bounds,
                              extend=colorbar_extend,
                              shrink=colorbar_shrink,
                              pad=colorbar_pad,
                              aspect=colorbar_aspect,
                              fraction=colorbar_fraction)

        if map2:
            self.add_submap()

    def configure_plot_defaults(self) -> None:
        plt.rcParams.update({
            'font.family': 'Times New Roman',
            'mathtext.default': 'regular',
            'axes.unicode_minus': False,
            'font.size': 12
        })

    ### *----------------------------------------* ###
    ### *---   plot the base background map   ---* ###
    ### *----------------------------------------* ###
    def set_extent(self, extent: list) -> None:
        self.ax.set_extent(extent)

    def set_title(self, title: str, **kwargs) -> None:
        self.ax.set_title(title, **kwargs)

    def add_coastline(self, ) -> None:
        self.ax.add_feature(cfeature.COASTLINE, lw=0.3)

    def add_china_border(self) -> None:

        self.ax.add_geometries(Reader(shp_dir + '/china1.shp').geometries(),
                               ccrs.PlateCarree(),
                               facecolor='none',
                               edgecolor='k',
                               linewidth=0.6)
        self.ax.add_geometries(Reader(shp_dir + '/china2.shp').geometries(),
                               ccrs.PlateCarree(),
                               facecolor='none',
                               edgecolor='k',
                               linewidth=0.1)

    def add_gridlines(self, ) -> None:

        gl = self.ax.gridlines(
            draw_labels=True,
            x_inline=False,
            y_inline=False,
            linewidth=0.1,
            color='gray',
            alpha=0.8,
            linestyle='--',
        )
        gl.top_labels = False
        gl.right_labels = False
        gl.xlocator = mticker.FixedLocator(range(80, 135, 10))
        gl.ylocator = mticker.FixedLocator(range(20, 50, 5))
        gl.ylabel_style = {'size': 12, 'color': 'k'}
        gl.xlabel_style = {'size': 12, 'color': 'k'}
        gl.rotate_labels = False

    @staticmethod
    def set_colormap(cmap: str, colors: list) -> object:

        return plt.get_cmap(cmap) if cmap is not None else ListedColormap(
            colors)

    @staticmethod
    def set_norm(bounds: list, cmap: list, colorbar_norm: str,
                 colorbar_extend: str) -> object:
        if colorbar_norm is None:
            return BoundaryNorm(bounds, cmap.N, extend=colorbar_extend)
        elif colorbar_norm == 'log':
            return LogNorm(vmin=np.min(bounds),
                           vmax=np.max(bounds),
                           extend=colorbar_extend)
        elif colorbar_norm in ('continous', 'c'):
            return Normalize(vmin=np.min(bounds), vmax=np.max(bounds))

    ### *-------------------------------* ###
    ### *---    Add some elements    ---* ###
    ### *-------------------------------* ###

    ### *--- add the colorbar ---* ###
    def add_colorbar(self,
                     cmap=None,
                     norm=None,
                     bounds=None,
                     extend=None,
                     shrink=0.9,
                     pad=0.01,
                     aspect=20,
                     fraction=0.03,
                     ticks_position='right',
                     orientation='vertical',
                     label=None,
                     label_loc='top',
                     **kwargs):

        cbar = plt.colorbar(mpl.cm.ScalarMappable(cmap=cmap or self.cmap,
                                                  norm=norm or self.norm),
                            ax=self.ax,
                            boundaries=bounds or self.bounds,
                            extend=extend or self.extend,
                            ticks=bounds or self.bounds,
                            shrink=shrink,
                            pad=pad,
                            aspect=aspect,
                            fraction=fraction,
                            orientation=orientation,
                            **kwargs)
        cbar.ax.yaxis.set_ticks_position(ticks_position)
        cbar.set_label(label, loc=label_loc)

    ### *--- add the southern ocean subplot ---* ###
    def add_submap(self) -> None:

        map2 = ccrs.LambertConformal(central_longitude=115,
                                     central_latitude=12.5,
                                     standard_parallels=(3, 20))
        ax2 = self.fig.add_axes([0.7, 0.14, 0.2, 0.2],
                                projection=map2)  # left,bottom,width,height
        ax2.set_extent([105.8, 122, 0, 25])
        ax2.add_feature(cfeature.OCEAN.with_scale('110m'))
        ax2.add_geometries(
            Reader('%s/china1.shp' % (shp_dir)).geometries(),
            ccrs.PlateCarree(),
            facecolor='none',
            edgecolor='k',
            linewidth=0.5,
        )
        ax2.add_geometries(
            Reader('%s/ne_10m_land.shp' % (shp_dir)).geometries(),
            ccrs.PlateCarree(),
            facecolor='none',
            edgecolor='k',
            linewidth=0.5,
        )
        lb2 = ax2.gridlines(
            draw_labels=False,
            x_inline=False,
            y_inline=False,
            linewidth=0.1,
            color='gray',
            alpha=0.8,
            linestyle='--',
        )
        lb2.xlocator = mticker.FixedLocator(range(90, 135, 5))
        lb2.ylocator = mticker.FixedLocator(range(0, 90, 5))

    ### *--- plot scatter ---* ###
    def scatter(self,
                lon: np.ndarray,
                lat: np.ndarray,
                value: np.ndarray,
                size=8,
                edgecolor='black',
                marker='.',
                alpha=1,
                linewidths=0.3,
                meshgrid=True,
                missing_value=np.nan,
                colors=None,
                bounds=None,
                **kwargs) -> None:

        if meshgrid:
            lon, lat = np.meshgrid(lon, lat)

        value = np.where(value == missing_value, np.nan, value)

        cmap, norm = (self.cmap,
                      self.norm) if colors is None or bounds is None else (
                          ListedColormap(colors),
                          BoundaryNorm(bounds, len(colors)))

        self.ax.scatter(lon,
                        lat,
                        s=size,
                        c=value,
                        edgecolor=edgecolor,
                        linewidths=linewidths,
                        transform=ccrs.PlateCarree(),
                        cmap=cmap,
                        norm=norm,
                        marker=marker,
                        alpha=alpha,
                        **kwargs)

    ### *--- plot line with smooth ---* ###
    def line(self,
             region_dict: dict,
             smooth=10,
             linewidth=1,
             color='black',
             **kwargs) -> None:

        for loc in region_dict.values():
            for i in range(0, len(loc) - 2, 2):
                lon_start, lat_start, lon_end, lat_end = loc[i:i + 4]
                lon_arr = np.linspace(lon_start, lon_end, smooth)
                lat_arr = np.linspace(lat_start, lat_end, smooth)
                self.ax.plot(lon_arr,
                             lat_arr,
                             c=color,
                             lw=linewidth,
                             transform=ccrs.PlateCarree(),
                             **kwargs)

    ### *--- plot contour ---* ###
    def contourf(self,
                 lon: np.ndarray,
                 lat: np.ndarray,
                 data: np.ndarray,
                 levels=None,
                 extend=None,
                 cmap=None,
                 norm=None,
                 meshgrid=True,
                 **kwargs) -> None:

        if meshgrid:
            lon, lat = np.meshgrid(lon, lat)

        self.ax.contourf(lon,
                         lat,
                         data,
                         levels=levels or self.bounds,
                         transform=ccrs.PlateCarree(),
                         cmap=cmap or self.cmap,
                         norm=norm or self.norm,
                         extend=extend or self.extend,
                         **kwargs)

    ### *--- plot quivers ---* ###
    def quiver(self,
               lon: np.ndarray,
               lat: np.ndarray,
               u: np.ndarray,
               v: np.ndarray,
               color='black',
               width=0.005,
               scale=200,
               headwidth=2,
               meshgrid=False,
               **kwargs) -> None:

        if meshgrid:
            lon, lat = np.meshgrid(lon, lat)

        self.quiver = self.ax.quiver(lon,
                                     lat,
                                     u,
                                     v,
                                     color=color,
                                     width=width,
                                     scale=scale,
                                     headwidth=headwidth,
                                     transform=ccrs.PlateCarree(),
                                     **kwargs)

    ### *--- add the key to the quivers ---* ###
    def quiverkey(self,
                  key_position=None,
                  length=10,
                  label='default',
                  labelsep=0.01,
                  labelpos='N',
                  fontproperties=None,
                  **kwargs) -> None:

        key_position = key_position or [0.95, 1.02]
        fontproperties = fontproperties or {
            'size': 10,
            'family': 'Times New Roman'
        }

        self.ax.quiverkey(self.quiver,
                          key_position[0],
                          key_position[1],
                          length,
                          label=label,
                          labelsep=labelsep,
                          labelpos=labelpos,
                          fontproperties=fontproperties,
                          **kwargs)

    ### *--- add text ---* ###
    def text(self,
             x_location: float,
             y_location: float,
             text_str: str,
             fontsize=12,
             **kwargs) -> None:

        self.ax.text(x=x_location,
                     y=y_location,
                     s=text_str,
                     transform=self.ax.transAxes,
                     fontsize=fontsize,
                     **kwargs)

    def legend(self, **kwargs):

        plt.legend(fontsize=12, **kwargs)

    ### *--- save figure ---* ###
    def save(self, name=None, display_legend=False, **kwargs) -> None:

        name = datetime.now().strftime(
            '%Y%m%d_%H%M%S') if name is None else name

        if display_legend:
            plt.legend(fontsize=12)

        plt.savefig(name, bbox_inches='tight', **kwargs)

        logging.info('Plot took %.2f s' %
                     ((datetime.now() - self.start).total_seconds()))

    def close(self):
        plt.close()


if __name__ == '__main__':

    import numpy as np
    import netCDF4 as nc

    logging.basicConfig(level=logging.INFO)
    bounds = [0, 100, 300, 600, 1000, 2000, 3000, 4000]
    colors = [
        '#F0F0F0', '#F0F096', '#FA9600', '#FA0064', '#9632FA', '#1414FA',
        '#0000b3', '#000000'
    ]

    with nc.Dataset(
            '/home/pangmj/Data/pyFilter/projects/ChinaDust20210315_meteo32/model_run/dust_conc-sfc.nc'
    ) as nc_obj:
        data = np.sum(nc_obj.variables['dust_conc'][30, :, :, :], axis=0)

    mmp = MyMap(title='Test title', bounds=bounds, colors=colors)
    mmp.contourf(np.arange(70, 140, 0.25), np.arange(15, 50, 0.25), data)
    mmp.scatter([110, 120], [25, 25], ['red'], size=100)
    mmp.scatter(np.arange(90, 100, 1),
                np.arange(20, 30, 1),
                np.random.random((10, 10)) * 1000,
                size=24)
    mmp.line({'test': [100, 25, 120, 35]})
    mmp.quiver(np.arange(90, 110, 2),
               np.arange(35, 55, 2),
               np.random.random((10, 10)) * 5,
               np.random.random((10, 10)) * 5,
               meshgrid=True,
               headwidth=0.5)
    mmp.quiverkey()
    mmp.text(0.5, 0.45, 'test')
    mmp.save('test.pdf')
