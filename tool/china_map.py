'''
Autor: Mijie Pang
Date: 2023-04-24 09:07:15
LastEditTime: 2024-04-17 19:23:15
Description: designed for a flexible map plot
'''
import os
import logging
import warnings
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.colors import ListedColormap, BoundaryNorm, LogNorm
from datetime import datetime
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.io.shapereader import Reader

from decorators import deprecated

warnings.filterwarnings(action='ignore')
shp_dir = '/home/pangmj/package/shp_file/china'


@deprecated
class my_map:

    def __init__(self,
                 figsize=[8, 5],
                 dpi=300,
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
                 title=datetime.now().strftime('Created on %Y-%m-%d %H:%M:%S'),
                 colorbar_extend='max',
                 colorbar_shrink=0.9,
                 colorbar_aspect=20,
                 colorbar_fraction=0.03,
                 colorbar_pad=0.01,
                 colorbar_norm='default',
                 extent=[80, 134.1, 15.5, 52.5],
                 map2=True,
                 display_colorbar=True,
                 display_gridline=True,
                 display_axis=True,
                 display_coastline=True,
                 colormap=None,
                 **kwargs) -> None:

        logging.info('Plot project initiated')

        logging.getLogger('matplotlib').setLevel(logging.WARNING)
        logging.getLogger('fiona').setLevel(logging.WARNING)

        ### initialize the figure ###
        fig = plt.figure(figsize=figsize, dpi=dpi)
        plt.rcParams['font.family'] = 'Times New Roman'
        plt.rcParams['mathtext.default'] = 'regular'
        plt.rcParams['axes.unicode_minus'] = False
        plt.rcParams['font.size'] = 12

        ### define the map projection ###
        projection = ccrs.LambertConformal(central_longitude=107.5,
                                           central_latitude=36.0,
                                           standard_parallels=(25, 47))
        ax = fig.add_subplot(1, 1, 1, projection=projection)
        ax.set_extent(extent)

        plt.tick_params(labelsize=12)

        if not title == False:
            ax.set_title(title, {'size': 14, 'color': 'k'})

        if not display_axis:
            plt.axis('off')

        if display_coastline:
            ax.add_feature(cfeature.COASTLINE, lw=0.3)
            # ax.add_feature(cfeature.OCEAN.with_scale('110m'))
            # ax.add_feature(cfeature.LAND.with_scale('110m'))
        # ax.add_geometries(
        #     Reader('/home/pangmj/package/shp_file/world/world.shp').geometries(),
        #     ccrs.PlateCarree(),
        #     facecolor='none',
        #     edgecolor='k',
        #     linewidth=0.1,
        # )

        ax.add_geometries(
            Reader(shp_dir + '/china1.shp').geometries(),
            ccrs.PlateCarree(),
            facecolor='none',
            edgecolor='k',
            linewidth=0.6,
        )
        ax.add_geometries(
            Reader(shp_dir + '/china2.shp').geometries(),
            ccrs.PlateCarree(),
            facecolor='none',
            edgecolor='k',
            linewidth=0.1,
        )

        # ax.add_geometries(
        #     Reader(shp_dir + '/bou1_4l.shp').geometries(),
        #     ccrs.PlateCarree(),
        #     facecolor='none',
        #     edgecolor='k',
        #     linewidth=0.6,
        # )

        # ax.add_geometries(
        #     Reader("{}/ne_10m_land.shp".format(shp_dir)).geometries(),
        #     ccrs.PlateCarree(),
        #     facecolor='none',
        #     edgecolor='k',
        #     linewidth=0.5,
        # )

        ### *--- grid line ---* ###
        if display_gridline:

            gl = ax.gridlines(
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

        if not colormap is None:
            cmap = colormap
        else:
            cmap = ListedColormap(colors)
        if colorbar_norm == 'default':
            norm = BoundaryNorm(bounds, cmap.N, extend=colorbar_extend)
        elif colorbar_norm == 'log':
            norm = LogNorm(vmin=np.min(bounds),
                           vmax=np.max(bounds),
                           extend=colorbar_extend)

        ### *--- colorbar ---* ###
        if display_colorbar:

            colorbar = plt.colorbar(
                mpl.cm.ScalarMappable(cmap=cmap, norm=norm),
                ax=ax,
                boundaries=bounds,  # Adding values for extensions.
                extend=colorbar_extend,  # {'neither', 'both', 'min', 'max'}
                ticks=bounds,
                # spacing = 'proportional',
                shrink=colorbar_shrink,
                pad=colorbar_pad,
                aspect=colorbar_aspect,  # aspect控制bar宽度
                fraction=colorbar_fraction,  # fraction控制大小比例
            )
            # print( color_bar.ax.get_position())

        ### *---------------------------* ###
        ### *---       subplot       ---* ###
        if map2:

            map2 = ccrs.LambertConformal(central_longitude=115,
                                         central_latitude=12.5,
                                         standard_parallels=(3, 20))
            ax2 = fig.add_axes([0.7, 0.14, 0.2, 0.2],
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

        self.norm = norm
        self.cmap = cmap
        self.colors = colors
        self.bounds = bounds
        self.start = datetime.now()
        self.fig = fig
        self.ax = ax
        self.plt = plt

    def add_colorbar(self,
                     bounds=None,
                     colors=None,
                     position=None,
                     extend='max',
                     ticks_position='left',
                     orientation='vertical',
                     label='',
                     **kwargs) -> None:

        if not bounds is None and not colors is None:
            cmap = ListedColormap(colors)
            norm = BoundaryNorm(bounds, cmap.N, extend=extend)
        else:
            colors = self.colors
            bounds = self.bounds

        position = self.fig.add_axes(position)
        cbar = plt.colorbar(
            mpl.cm.ScalarMappable(cmap=cmap, norm=norm),
            ax=self.ax,
            boundaries=bounds,  # Adding values for extensions.
            extend=extend,  # {'neither', 'both', 'min', 'max'}
            orientation=orientation,
            label=label,
            ticks=bounds,
            cax=position,
            **kwargs)
        cbar.ax.yaxis.set_ticks_position(ticks_position)
        # print(cbar.ax.get_position())

    ### *--- plot scatter ---* ###
    def scatter(self,
                lon: np.ndarray,
                lat: np.ndarray,
                value: np.ndarray,
                color='',
                edgecolor='black',
                size=8,
                marker='.',
                alpha=1,
                linewidths=0.3,
                meshgrid=True,
                missing_value='',
                colors=None,
                bounds=None,
                **kwargs) -> None:

        if meshgrid:
            lon, lat = np.meshgrid(lon, lat)

        if not color == '':
            value = color

        if not missing_value == '':
            value[value == missing_value] = np.nan

        if colors is None or bounds is None:
            norm = self.norm
            cmap = self.cmap
        else:
            cmap = ListedColormap(colors)
            norm = BoundaryNorm(bounds, cmap.N)

        ax = plt.gca()

        ax.scatter(lon,
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

    ### *--- plot line ---* ###
    def line(self,
             region_dict: dict,
             smooth=20,
             linewidth=1,
             color='black',
             **kwargs) -> None:

        ax = plt.gca()

        for key in region_dict:
            loc = region_dict[key]
            num = int(len(loc) / 2) - 1
            for j in range(num):
                k = 2 * j
                for s in range(smooth):
                    start_lon = loc[k] + (loc[k + 2] - loc[k]) * (s / smooth)
                    end_lon = loc[k] + (loc[k + 2] - loc[k]) * (s + 1) / smooth
                    start_lat = loc[k + 1] + (loc[k + 3] -
                                              loc[k + 1]) * (s / smooth)
                    end_lat = loc[k + 1] + (loc[k + 3] - loc[k + 1]) * (
                        (s + 1) / smooth)
                    ax.plot([start_lon, end_lon], [start_lat, end_lat],
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
                 meshgrid=True,
                 **kwargs) -> None:

        if meshgrid:
            lon, lat = np.meshgrid(lon, lat)

        ax = plt.gca()
        c1 = ax.contourf(lon,
                         lat,
                         data,
                         levels=levels,
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

        ax = plt.gca()
        self.quiver = ax.quiver(lon,
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
                  key_position=[0.95, 1.02],
                  length=10,
                  label='default',
                  labelsep=0.01,
                  labelpos='N',
                  font={},
                  **kwargs) -> None:

        default_font = {'size': 10, 'family': 'Times New Roman'}
        default_font.update(font)
        ax = plt.gca()
        ax.quiverkey(self.quiver,
                     key_position[0],
                     key_position[1],
                     length,
                     label=label,
                     labelsep=labelsep,
                     labelpos=labelpos,
                     fontproperties=default_font,
                     **kwargs)

    ### *--- add text ---* ###
    def text(self, location: list, text: str, fontsize=12, **kwargs) -> None:

        x_location, y_location = location
        ax2 = plt.gca()
        plt.text(x_location,
                 y_location,
                 text,
                 transform=ax2.transAxes,
                 fontsize=fontsize,
                 **kwargs)

    ### *--- save figure ---* ###
    def save(self,
             name='Dafault',
             path='.',
             create_dir=False,
             display_legend=False,
             **kwargs) -> None:

        if display_legend:
            plt.legend(fontsize=12)

        if create_dir:
            if not os.path.exists(path):
                os.makedirs(path)

        if not name.endswith(('.jpg', '.png', '.tif', '.tiff', '.svg', '.svgz',
                              '.eps', '.pdf', '.pgf', '.ps', '.raw', '.rgba')):

            name = name + '.png'

        plt.savefig(os.path.join(path, name), bbox_inches='tight', **kwargs)

        logging.info('Plot took %.2f s' %
                     ((datetime.now() - self.start).total_seconds()))

    def close(self):
        plt.close()


if __name__ == '__main__':

    mmp = my_map()
    mmp.scatter(110, 30, 100)
    mmp.line({'line': [85, 31, 130, 50]})
    mmp.save('test.png')
