import os
import seaborn as sns
import matplotlib.pyplot as plt

def save_chart(fig_name: str, plot_func, *args, **kwargs):
    """
    Saves a Seaborn/Matplotlib chart to a 'charts' folder in the parent directory.

    Parameters:
        fig_name (str): Name of the file to save (without extension).
        plot_func (callable): The plotting function (e.g., sns.histplot, sns.barplot).
        *args: Positional args for the plot function.
        **kwargs: Keyword args for the plot function.
    """
    # 1. Set theme and palette
    sns.set_theme(style="whitegrid", palette="deep")
    sns.set_context("notebook", font_scale=1.2)

    # 2. Create save directory in parent folder
    save_dir = os.path.join(os.path.dirname(os.getcwd()), "charts")
    os.makedirs(save_dir, exist_ok=True)

    # 3. Create plot
    plt.figure(figsize=(8, 5))
    plot_func(*args, **kwargs)

    # 4. Save plot
    save_path = os.path.join(save_dir, f"{fig_name}.png")
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.show()

    print(f"✅ Chart saved to: {save_path}")