from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import numpy as np
# set up orthographic map projection with
# perspective of satellite looking down at 50N, 100W.
# use low resolution coastlines.
map = Basemap(projection='merc',
            lat_0=35,
            lon_0=-70,
            resolution='i',
            llcrnrlon=-130,
            llcrnrlat=25,
            urcrnrlon=-60,
            urcrnrlat=50)
# draw coastlines, country boundaries, fill continents.
map.drawcoastlines(linewidth=0.25,color='#555555')
map.drawcountries(linewidth=0.45)
map.fillcontinents(color='white',lake_color='#a2c5ff')
map.drawlsmask(ocean_color='#a2c5ff')
map.drawstates(linewidth=0.2,color='gray')

# draw the edge of the map projection region (the projection limb)
# map.drawmapboundary(fill_color='aqua')


# draw lat/lon grid lines every 30 degrees.
#map.drawmeridians(np.arange(0,360,30),linewidth=0.2,dashes=[10,1,2,1],color='#999999')

#map.drawparallels(np.arange(-90,90,30),linewidth=0.2,dashes=[10,1,2,1],color='#999999')
# make up some data on a regular lat/lon grid.
nlats = 73; nlons = 145; delta = 2.*np.pi/(nlons-1)
lats = (0.5*np.pi-delta*np.indices((nlats,nlons))[0,:,:])
lons = (delta*np.indices((nlats,nlons))[1,:,:])
wave = 0.75*(np.sin(2.*lats)**8*np.cos(4.*lons))
mean = 0.5*np.cos(2.*lats)*((np.sin(2.*lats))**2 + 2.)
# compute native map projection coordinates of lat/lon grid.
x, y = map(lons*180./np.pi, lats*180./np.pi)
# contour data over the map.
#cs = map.contour(x,y,wave+mean,15,linewidths=1.5)
plt.title('contour lines over filled continent background')
plt.savefig('map-us.pdf')
#plt.show()
