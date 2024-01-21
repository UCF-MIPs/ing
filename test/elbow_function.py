"""
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.
Equivalent R code found at: 
                    https://github.com/ahasverus/elbow
                    R code author : Nicolas Casajus (2020)
This python version authored by: Chathura Jayalath (2023)
"""


import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt

def get_elbow(in_xvals, in_yvals, in_plot=False, in_image_path="./elbow.png"):
    tdf = pd.DataFrame({"x": in_xvals, "y": in_yvals})
    model = LinearRegression().fit(tdf["x"].values.reshape((-1, 1)), tdf["y"].values)
    tdf["constant"] = model.intercept_ + model.coef_ * tdf["x"]
    pos = tdf.shape[0] // 2 - 1
    if tdf.loc[pos, "constant"] < tdf.loc[pos][1]:
        print("min")
        ymin = min(tdf.iloc[:, 1])
        tdf["benefits"] = ymin + (tdf.iloc[:, 1] - tdf["constant"])
        maxi = tdf.loc[tdf["benefits"].idxmax()]
    else:
        print("max")
        ymax = max(tdf.iloc[:, 1])
        tdf["benefits"] = ymax - (tdf["constant"] - tdf.iloc[:, 1])
        maxi = tdf.loc[tdf["benefits"].idxmin()]
    if in_plot:
        fig1, ax1 = plt.subplots()
        ax1.plot(tdf["x"].values, tdf["y"].values, label="data")
        ax1.plot(tdf["x"].values, tdf["benefits"].values, label="benefits")
        ax1.scatter(x=maxi["x"], y= maxi["y"], c='r') #label='Elbow Point')
        ax1.scatter(x=maxi["x"], y= maxi["benefits"], c='r')#, label='Elbow Est.')
        ax1.axvline(maxi["x"], color='red', linewidth=.5)
        ax1.text(maxi["x"], maxi["y"], "{:.2f} , {:.2f}".format(maxi["x"],maxi["y"]), fontsize = 20) 
        plt.legend()
        plt.savefig(in_image_path)
        # plt.show()
    print(f"idx: {maxi.name}")
    return maxi
 