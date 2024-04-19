'''
Autor: Mijie Pang
Date: 2024-03-21 15:59:17
LastEditTime: 2024-04-17 19:59:07
Description: 
'''
import logging
import warnings
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.colors import (ListedColormap, Normalize, BoundaryNorm,
                               LogNorm, LinearSegmentedColormap)
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.io.shapereader import Reader
from datetime import datetime

warnings.filterwarnings(action='ignore')
shp_dir = '/home/pangmj/package/shp_file/china'


class MyMapMulti:

    def __init__(self,
                 figsize=(8, 5),
                 dpi=300,
                 extent=None,
                 colors=None,
                 bounds=None,
                 draft=False):

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

        self.configure_plot_defaults()
        self.projection = ccrs.LambertConformal(central_longitude=107.5,
                                                central_latitude=36.0,
                                                standard_parallels=(25, 47))

        fig = plt.figure(figsize=figsize, dpi=dpi)
        # fig.tight_layout()
        # plt.subplots_adjust(hspace=hspace, wspace=wspace)

        self.fig = fig
        self.extent = extent
        self.colors = colors
        self.bounds = bounds
        self.draft = draft

    def configure_plot_defaults(self) -> None:
        plt.rcParams.update({
            'font.family': 'Times New Roman',
            'mathtext.default': 'regular',
            'axes.unicode_minus': False,
            'font.size': 12,
            'figure.autolayout': True
        })

    ### *--- Subplot method ---* ###
    def subplot(
        self,
        f1: int,
        f2: int,
        num: int,
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
        extend='max',
        shrink=0.9,
        aspect=20,
        fraction=0.03,
        pad=0.01,
        norm_type=None,
    ) -> None:

        print('Subplot created : %s / %s' % (num, f1 * f2))

        self.ax = self.fig.add_subplot(f1, f2, num, projection=self.projection)

        self.set_extent(extent or self.extent)
        self.set_title(title)

        if coastline:
            self.add_coastline()

        if not self.draft:
            self.add_china_border()

        if gridline:
            self.add_gridlines()

        cmap = self.set_colormap(cmap,
                                 colors or self.colors,
                                 cmap_type=norm_type)
        norm = self.set_norm(bounds or self.bounds, cmap, norm_type, extend)
        self.cmap = cmap
        self.norm = norm
        self.bounds = bounds or self.bounds

        if colorbar:
            self.add_colorbar(self.cmap,
                              self.norm,
                              bounds or self.bounds,
                              extend=extend,
                              shrink=shrink,
                              pad=pad,
                              aspect=aspect,
                              fraction=fraction)

        if not axis:
            self.ax.axis('off')

        if map2 and not self.draft:
            self.add_submap()

    ### *--- Grid spec method ---* ###
    def grid_spec_plot(self, ):
        pass

    ### *----------------------------------------* ###
    ### *---   plot the base background map   ---* ###
    ### *----------------------------------------* ###
    def set_extent(self, extent: list) -> None:
        self.ax.set_extent(extent)

    def set_title(self, title: str, **kwargs) -> None:
        self.ax.set_title(title, **kwargs)

    def add_coastline(self, ) -> None:
        self.ax.add_feature(cfeature.COASTLINE, lw=0.3)

    def add_china_border(self, ) -> None:

        self.ax.add_geometries(Reader(shp_dir + '/china1.shp').geometries(),
                               ccrs.PlateCarree(),
                               facecolor='none',
                               edgecolor='k',
                               linewidth=0.9)
        self.ax.add_geometries(Reader(shp_dir + '/china2.shp').geometries(),
                               ccrs.PlateCarree(),
                               facecolor='none',
                               edgecolor='k',
                               linewidth=0.1)

    def add_gridlines(self,
                      left_label=True,
                      bottom_label=True,
                      top_label=False,
                      right_label=False,
                      **kwargs) -> object:

        gl = self.ax.gridlines(draw_labels=True,
                               x_inline=False,
                               y_inline=False,
                               linewidth=0.1,
                               color='gray',
                               alpha=0.8,
                               linestyle='--',
                               **kwargs)
        gl.top_labels = top_label
        gl.right_labels = right_label
        gl.xlabels_bottom = bottom_label
        gl.ylabels_left = left_label

        gl.xlocator = mticker.FixedLocator(range(80, 135, 10))
        gl.ylocator = mticker.FixedLocator(range(20, 50, 5))
        gl.ylabel_style = {'size': 16, 'color': 'k'}
        gl.xlabel_style = {'size': 16, 'color': 'k'}
        gl.rotate_labels = False

        return gl

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

    ### *-------------------------------* ###
    ### *---    Add some elements    ---* ###
    ### *-------------------------------* ###

    ### *--- add the colorbar ---* ###
    def add_colorbar(self,
                     cmap=None,
                     norm=None,
                     bounds=None,
                     extend='max',
                     shrink=0.9,
                     pad=0.01,
                     aspect=20,
                     fraction=0.03,
                     ticks_position='right',
                     orientation='vertical',
                     label=None,
                     **kwargs):

        cbar = plt.colorbar(mpl.cm.ScalarMappable(cmap=cmap or self.cmap,
                                                  norm=norm or self.norm),
                            ax=self.ax,
                            boundaries=bounds or self.bounds,
                            extend=extend,
                            ticks=bounds or self.bounds,
                            shrink=shrink,
                            pad=pad,
                            aspect=aspect,
                            fraction=fraction,
                            orientation=orientation,
                            **kwargs)
        cbar.ax.yaxis.set_ticks_position(ticks_position)
        cbar.ax.text(1.4, 1.15, label, transform=cbar.ax.transAxes)

        return cbar

    ### *--- add a global colorbar ---* ###
    def add_colorbar_global(self,
                            position=[1, 0.2, 0.008, 0.6],
                            cmap=None,
                            norm=None,
                            colors=None,
                            bounds=None,
                            norm_type=None,
                            extend='max',
                            shrink=0.9,
                            pad=0.01,
                            aspect=20,
                            fraction=0.03,
                            ticks_position='right',
                            orientation='vertical',
                            label=None,
                            **kwargs) -> None:

        if not colors is None and not bounds is None:
            cmap = self.set_colormap(cmap,
                                     colors or self.colors,
                                     cmap_type=norm_type)
            norm = self.set_norm(bounds or self.bounds, cmap, norm_type,
                                 extend)

        cax = self.fig.add_axes(position)  # left, bottom, width, height
        cbar = self.fig.colorbar(mpl.cm.ScalarMappable(cmap=cmap or self.cmap,
                                                       norm=norm or self.norm),
                                 cax=cax,
                                 boundaries=bounds or self.bounds,
                                 extend=extend,
                                 ticks=bounds or self.bounds,
                                 shrink=shrink,
                                 pad=pad,
                                 aspect=aspect,
                                 fraction=fraction,
                                 orientation=orientation,
                                 **kwargs)
        cbar.ax.yaxis.set_ticks_position(ticks_position)
        cbar.ax.tick_params(labelsize=18)
        # cbar.ax.text(0.5, 1.1, label, transform=cbar.ax.transAxes, fontsize=20)

        return cbar

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
    def contourf(
            self,
            lon: np.ndarray,
            lat: np.ndarray,
            data: np.ndarray,
            levels=None,
            #  cmap=None,
            #  norm=None,
            meshgrid=True,
            **kwargs) -> None:

        if meshgrid:
            lon, lat = np.meshgrid(lon, lat)

        self.ax.contourf(lon,
                         lat,
                         data,
                         levels=levels or self.bounds,
                         transform=ccrs.PlateCarree(),
                         cmap=self.cmap,
                         norm=self.norm,
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
             fontsize=16,
             **kwargs) -> None:

        self.ax.text(x=x_location,
                     y=y_location,
                     s=text_str,
                     transform=self.ax.transAxes,
                     fontsize=fontsize,
                     **kwargs)

    ### *--- save figure ---* ###
    def save(self, name=None, display_legend=False, **kwargs) -> None:

        name = datetime.now().strftime(
            '%Y%m%d_%H%M%S') if name is None else name

        if display_legend:
            plt.legend(fontsize=12)

        plt.savefig(name, bbox_inches='tight', **kwargs)
        # subprocess.run(['mv', name + '_', name])

        logging.info('Plot took %.2f s' %
                     ((datetime.now() - self.start).total_seconds()))

    def close(self):
        plt.close()


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
    mmpm = MyMapMulti(figsize=[14, 10], draft=False)

    mmpm.subplot(2, 2, 1, colorbar=False)
    mmpm.set_title('Subplot-%s' % (1))
    mmpm.line({'line': [80, 25, 130, 31]})

    mmpm.subplot(2, 2, 2, colorbar=False)
    mmpm.set_title('Subplot-%s' % (2))
    mmpm.scatter(110, 35, ['k'], size=100)

    mmpm.subplot(2, 2, 3, colorbar=False)
    mmpm.set_title('Subplot-%s' % (3))

    mmpm.subplot(2, 2, 4, colorbar=False)
    mmpm.set_title('Subplot-%s' % (4))
    mmpm.line({'line': [85, 31, 130, 50]})

    cbar = mmpm.add_colorbar_global(position=[1, 0.2, 0.015, 0.6],
                                    label='$\mu g/m^{3}$')
    cbar.ax.text(4,
                 0.5,
                 'Concentration',
                 transform=cbar.ax.transAxes,
                 fontsize=20,
                 rotation=-90)

    mmpm.save('test_multi.pdf')
