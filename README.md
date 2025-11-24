The Protect Track Cover plugin for MusicBrainz Picard warns when individual tracks on an album use different covers and there is a risk that these could be replaced by the album image during tagging. However, it does not actively block the overwriting of covers, but offers a warning function to alert users to potential risks. The target audience is users who value individual covers per track and do not want special or different covers to be accidentally lost when standardising album covers.

You can find more details in the source code:
[protect_track_cover/__init__.py](https://github.com/nrth3rnlb/picard-plugin-protect_track_cover/blob/ffb88b183f2aa93fc99f42282c91716728fffa03/protect_track_cover/__init__.py)

In order to display images, we need the Python Image Library.


For Flatpak installations


```
flatpak run --command=sh org.musicbrainz.Picard


python -m ensurepip --upgrade
python -m pip install --user pillow
```


There's probably an easier way to do it, but this works too.
