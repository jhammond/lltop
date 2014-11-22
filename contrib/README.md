Lustrejobtop
--------------

Take the output from lltop and map it to torque jobs. If the lustre
client names are different than the torque node names one must do the
appropriate mapping in the function lustre_to_torque().

The output looks something like this

```
# python ./lustrejobtop.py 
top writers
host          write MB    read MB     reqs   jobid(user)
   cib44-4       1663        200       3740    2273289(userA)
   cib4-13       1567        390       3968    2272148(userA)
   cib52-5       1456          0       2913    2282404(userB)
    cib4-3       1455        226       3674    2269783(userA)
   cib1-11        889          0       1783    2279077(userC)
    cib2-7        761        333       2274    2279082(userC)
   cib12-5        686        258       1892    2265178(userA)
  cib19-10        502       1850       4707    2236324(userD)    2252649(userD)
   cib55-1        350          0        718    2277005(userE)
   cib11-3        329          7       2730    2219852(userE)
top readers
host          write MB    read MB     reqs   jobid(user)
   cib19-3          0       1955       4159    2277674(userB)
   cib60-4          0       1871       4084    2275433(userB)
  cib19-10        502       1850       4707    2236324(userD)    2252649(userD)
   cib43-5        286       1735       5103    2278431(userE)
   cib33-4        259       1732       4340    2278431(userE)
   cib19-8          0       1318       2955    2277320(userB)
   cib1-14          0       1268       3927    2269780(userA)
  cib14-13          0       1174       2398    2265053(userB)
    cib7-9        142       1012       2456    2283945(userE)
   cib4-14          0        967       3011    2269788(userA)
top iops
host          write MB    read MB     reqs   jobid(user)
   cib43-5        286       1735       5103    2278431(userE)
  cib19-10        502       1850       4707    2236324(userD)    2252649(userD)
   cib33-4        259       1732       4340    2278431(userE)
   cib19-3          0       1955       4159    2277674(userB)
   cib60-4          0       1871       4084    2275433(userB)
   cib4-13       1567        390       3968    2272148(userA)
   cib1-14          0       1268       3927    2269780(userA)
   cib44-4       1663        200       3740    2273289(userA)
    cib4-3       1455        226       3674    2269783(userA)
  cib17-15          0        758       3558    2281925(userE)
Total: writes 11648 MB, reads 22491 MB, iops 81232
```
