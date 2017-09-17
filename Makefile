ramdisk : hello_patch.c
	gcc -Wall hello_patch.c `pkg-config fuse --cflags --libs` -o ramdisk
