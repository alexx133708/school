from tkinter import *

root = Tk()

list_cb = []
for j in range(10):
    list_cb.append(IntVar())

def print_list_cb():
    list_of_cb_values = []
    for i in range(10):
        list_of_cb_values.append(list_cb[i].get())
    print(list_of_cb_values)

for i in range(10):
    cb = Checkbutton(root, height=2, variable=list_cb[i])
    cb.pack()

btn = Button(root, text='print', command=print_list_cb)
btn.pack()

root.mainloop()